"""集中维护的 PyArrow 表 schema (README §8 schemas/)。

离线: doc_raw → doc_filtered → doc_dedup → doc_scored → corpus shard
在线: turn_candidate → scored_candidate → selected_prompt
      → response_candidates → preference_pair → rlhf_example
"""
import pyarrow as pa

DOC_RAW = pa.schema([
    ("doc_id", pa.string()),
    ("url", pa.string()),
    ("warc_file", pa.string()),
    ("warc_offset", pa.int64()),
    ("dump_id", pa.string()),
    ("text", pa.string()),
    ("extractor", pa.string()),
    ("fetch_ts", pa.timestamp("ms")),
])

DOC_FILTERED = DOC_RAW.append(pa.field("lang", pa.string())) \
    .append(pa.field("lang_conf", pa.float32())) \
    .append(pa.field("filter_flags", pa.map_(pa.string(), pa.bool_()))) \
    .append(pa.field("kept", pa.bool_()))

DOC_DEDUP = DOC_FILTERED.append(pa.field("cluster_id", pa.string())) \
    .append(pa.field("cluster_size", pa.int32())) \
    .append(pa.field("dup_of", pa.string()))

DOC_SCORED = DOC_DEDUP.append(pa.field("quality_score", pa.float32())) \
    .append(pa.field("classifier_version", pa.string()))

TURN_CANDIDATE = pa.schema([
    ("conversation_id", pa.string()),
    ("turn_id", pa.string()),
    ("model_version", pa.string()),
    ("prompt", pa.string()),
    ("response", pa.string()),
    ("user_edit", pa.string()),
    ("thumbs", pa.int8()),          # -1 / 0 / 1
    ("regenerated", pa.bool_()),
    ("stopped", pa.bool_()),
    ("followup_correction", pa.bool_()),
    ("late", pa.bool_()),           # 迟到反馈在 allowed lateness 内被合入
    ("event_ts", pa.timestamp("ms")),
    ("dt", pa.string()),
])

SCORED_CANDIDATE = TURN_CANDIDATE.append(pa.field("reward", pa.float32())) \
    .append(pa.field("safety", pa.float32())) \
    .append(pa.field("embedding", pa.list_(pa.float32()))) \
    .append(pa.field("cluster", pa.int32())) \
    .append(pa.field("novelty", pa.float32()))

SELECTED_PROMPT = pa.schema([
    ("conversation_id", pa.string()),
    ("turn_id", pa.string()),
    ("prompt", pa.string()),
    ("reason", pa.string()),  # negative | divergence | low_conf | new_cluster
    ("cluster", pa.int32()),
])

RESPONSE_CANDIDATES = pa.schema([
    ("turn_id", pa.string()),
    ("prompt", pa.string()),
    ("source", pa.string()),  # current_model | strong_model | user_edit | constitutional
    ("response", pa.string()),
])

PREFERENCE_PAIR = pa.schema([
    ("pair_id", pa.string()),
    ("conversation_id", pa.string()),
    ("turn_id", pa.string()),
    ("prompt", pa.string()),
    ("chosen", pa.string()),
    ("rejected", pa.string()),
    ("chosen_source", pa.string()),
    ("rejected_source", pa.string()),
    ("judge_conf", pa.float32()),
    ("judge_route", pa.string()),  # auto | human
])

RLHF_EXAMPLE = PREFERENCE_PAIR.append(pa.field("dataset_version", pa.string()))

CORPUS_MANIFEST = pa.schema([
    ("manifest_id", pa.string()),
    ("layer", pa.string()),
    ("doc_count", pa.int64()),
    ("token_count", pa.int64()),
    ("dump_distribution", pa.string()),
    ("classifier_version", pa.string()),
    ("rules_version", pa.string()),
])
