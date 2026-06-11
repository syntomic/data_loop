"""SVG 流程图 Demo 后端 (纯标准库 http.server, 无新依赖)。

数据逻辑全在 dataloop.webapp.data (与 Streamlit explorer 共用), 本模块只做 HTTP:
  GET /api/pipeline            九模块拓扑 + 各阶段真实行数/指标
  GET /api/table/<name>        某表的 schema + 抽样行 + Lance 物理 version
  GET /api/registry            数据集版本 / 快照绑定 / 消融报告
  GET /api/lineage/<kind>/<id> 血缘反查
  GET /                        前端单页 (static/index.html)

启动: uv run dataloop-webapp           (默认 http://127.0.0.1:8000)
      uv run python -m dataloop.webapp.server 8020
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from . import data as D

STATIC = Path(__file__).resolve().parent / "static"
_CFG = D.default_cfg()


class Handler(BaseHTTPRequestHandler):
    def _send(self, obj, ctype="application/json"):
        body = obj if isinstance(obj, bytes) else json.dumps(
            obj, ensure_ascii=False, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", ctype + ("; charset=utf-8" if "json" in ctype or "html" in ctype else ""))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _err(self, code, msg):
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps({"error": msg}, ensure_ascii=False).encode())

    def do_GET(self):
        path = unquote(urlparse(self.path).path)
        try:
            if path in ("/", "/index.html"):
                return self._send((STATIC / "index.html").read_bytes(), "text/html")
            if path == "/api/pipeline":
                return self._send(D.pipeline(_CFG))
            if path == "/api/registry":
                return self._send(D.registry(_CFG))
            if path == "/api/sample-ids":
                return self._send(D.sample_ids(_CFG))
            if path.startswith("/api/table/"):
                return self._send(D.table(_CFG, path[len("/api/table/"):]))
            if path.startswith("/api/lineage/"):
                rest = path[len("/api/lineage/"):]
                if "/" not in rest:
                    return self._err(400, "用法: /api/lineage/<kind>/<id>")
                kind, ident = rest.split("/", 1)
                return self._send(D.lineage(_CFG, kind, ident))
            return self._err(404, f"no route: {path}")
        except FileNotFoundError as e:
            return self._err(404, str(e))
        except Exception as e:  # noqa
            return self._err(500, f"{type(e).__name__}: {e}")

    def log_message(self, *a):  # 静默
        pass


def main(host: str = "127.0.0.1", port: int = 8000):
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        port = int(sys.argv[1])
    srv = ThreadingHTTPServer((host, port), Handler)
    print(f"data-loop 流程图: http://{host}:{port}  (Ctrl-C 退出)")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()
