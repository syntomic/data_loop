"""M7 signal-ingest 的 PyFlink DataStream 实现 (README §5.1)。

与 job.py 的文件 replay 模拟语义一致:
- conversation_id+turn_id keyBy, keyed state + event-time timer
- watermark: bounded out-of-orderness 5min
- allowed lateness 30min: timer 触发后保留状态, 迟到反馈在窗口内合入并标 late
- state TTL 24h; consent 维表 lookup join; PII 清洗 UDF
输出 TurnCandidate, 与 replay 后端逐行一致 (tests/test_m7_flink.py 校验)。
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import KeyedProcessFunction, RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from common.config import resolve
from common.lake import Lake
from schemas.tables import TURN_CANDIDATE
from .pii import scrub


def _read_jsonl(p: Path):
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()]


def _build_env(m: dict) -> StreamExecutionEnvironment:
    """MiniCluster(本地) 或 远端 Flink 集群 (jobmanager 来自 configs)。"""
    jm = m.get("jobmanager")
    if jm:
        from pyflink.common import Configuration
        conf = Configuration()
        host, _, port = jm.partition(":")
        conf.set_string("rest.address", host)
        conf.set_string("rest.port", port or "8081")
        env = StreamExecutionEnvironment.get_execution_environment(conf)
    else:
        env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(m.get("parallelism", 1))
    return env


def _kafka_source(env, cfg: dict, m: dict):
    """从 message_events / feedback_events 两个 topic 读事件 (cluster)。"""
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.typeinfo import Types as T

    def build(topic, tag):
        src = (KafkaSource.builder()
               .set_bootstrap_servers(m["kafka"])
               .set_topics(topic)
               .set_group_id(m.get("group_id", "data-loop-m7"))
               .set_starting_offsets(KafkaOffsetsInitializer.earliest())
               .set_value_only_deserializer(SimpleStringSchema()).build())
        return (env.from_source(src, WatermarkStrategy.no_watermarks(), f"kafka-{topic}")
                .map(lambda s, tag=tag: json.loads(s) | {"_topic": tag}, output_type=T.PICKLED_BYTE_ARRAY()))

    return build(m["message_topic"], "message").union(build(m["feedback_topic"], "feedback"))


class _ArrivalTs(TimestampAssigner):
    def extract_timestamp(self, ev, record_ts):
        return ev["ts"]


class _TurnAggregate(KeyedProcessFunction):
    """keyed state 聚合一个 turn 的 message + feedback 信号。"""

    def __init__(self, watermark_ms: int, lateness_ms: int, ttl_ms: int):
        self.watermark_ms, self.lateness_ms, self.ttl_ms = watermark_ms, lateness_ms, ttl_ms
        self.state = None

    def open(self, ctx: RuntimeContext):
        desc = ValueStateDescriptor("turn", Types.PICKLED_BYTE_ARRAY())
        ttl = StateTtlConfig.new_builder(Time.milliseconds(self.ttl_ms)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite).build()
        desc.enable_time_to_live(ttl)
        self.state = ctx.get_state(desc)

    def process_element(self, ev, ctx):
        s = self.state.value()
        if ev["_topic"] == "message":
            timer = ev["event_ts"] + self.watermark_ms
            s = {"conversation_id": ev["conversation_id"], "turn_id": ev["turn_id"],
                 "model_version": ev["model_version"],
                 "prompt": scrub(ev["prompt"]), "response": scrub(ev["response"]),
                 "user_edit": None, "thumbs": 0, "regenerated": False, "stopped": False,
                 "followup_correction": False, "late": False,
                 "event_ts": ev["event_ts"], "timer": timer, "expire": ev["ts"] + self.ttl_ms}
            ctx.timer_service().register_event_time_timer(timer + self.lateness_ms)
        else:
            if s is None or ev["ts"] > s["expire"] or ev["ts"] > s["timer"] + self.lateness_ms:
                return
            sig = ev["signal"]
            if sig == "thumbs_up":
                s["thumbs"] = 1
            elif sig == "thumbs_down":
                s["thumbs"] = -1
            elif sig == "regenerate":
                s["regenerated"] = True
            elif sig == "stop":
                s["stopped"] = True
            elif sig == "edit":
                s["user_edit"] = scrub(ev.get("edit_text"))
            elif sig == "followup_correction":
                s["followup_correction"] = True
            # 与 replay 后端一致的确定性迟到判定 (不依赖周期性 watermark 推进)
            if ev["ts"] - self.watermark_ms >= s["timer"]:
                s["late"] = True
        self.state.update(s)

    def on_timer(self, ts, ctx):
        s = self.state.value()
        if s is not None:
            yield {k: v for k, v in s.items() if k not in ("timer", "expire")}
            self.state.clear()


def run(cfg: dict) -> str:
    m = cfg["m7_signal_ingest"]
    env = _build_env(m)
    wm = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(m["watermark_ms"])) \
        .with_timestamp_assigner(_ArrivalTs())

    if m.get("source", "file_replay") == "kafka":
        # 集群: Kafka 无界源, consent 维表 lookup join 在 UDF 内做 (此处略, 接业务实现)
        keyed = _kafka_source(env, cfg, m).assign_timestamps_and_watermarks(wm)
    else:
        # 本地: 文件 replay 有界源, consent 隐私闸先做维表 join
        consent = {c["user_id"]: c["consent_ok"] for c in _read_jsonl(resolve(cfg, m["consent_table"]))}
        events = sorted(
            [e | {"_topic": "message"} for e in _read_jsonl(resolve(cfg, m["message_events"]))] +
            [e | {"_topic": "feedback"} for e in _read_jsonl(resolve(cfg, m["feedback_events"]))],
            key=lambda e: e["ts"])
        events = [e for e in events if consent.get(e["user_id"], False)]
        keyed = env.from_collection(events).assign_timestamps_and_watermarks(wm)

    stream = keyed.key_by(lambda e: f"{e['conversation_id']}:{e['turn_id']}") \
        .process(_TurnAggregate(m["watermark_ms"], m["allowed_lateness_ms"], m["state_ttl_ms"]))

    rows = [s | {"dt": datetime.fromtimestamp(s["event_ts"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")}
            for s in stream.execute_and_collect()]
    Lake(cfg).write("turn_candidate", rows, TURN_CANDIDATE, partition="dt")
    return "turn_candidate"
