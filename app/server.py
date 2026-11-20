"""纯 Python 标准库 HTTP 服务端入口。

用于在无 uvicorn/fastapi 三方库环境下（或 AI Studio 预览容器 3000 端口）
直接运行 app.main 中注册的全部路由与静态资源；本地完整安装依赖时仍可继续使用
`uvicorn app.main:app`。
"""
import argparse
import asyncio
import email.parser
import email.policy
import inspect
import json
import mimetypes
import os
import re
import signal
import sys
import types
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Union, get_args, get_origin, get_type_hints

from . import db, main


def _compile_routes():
    compiled = []
    for r in getattr(main.app, "routes", []):
        methods = getattr(r, "methods", None) or set()
        path = getattr(r, "path", "")
        endpoint = getattr(r, "endpoint", None)
        if not path or not endpoint:
            continue
        pattern = "^" + re.sub(r"\{([A-Za-z_][A-Za-z0-9_]*)\}", r"(?P<\1>[^/]+)", path) + "$"
        try:
            hints = get_type_hints(endpoint)
        except Exception:
            hints = getattr(endpoint, "__annotations__", {})
        compiled.append((methods, re.compile(pattern), endpoint, inspect.signature(endpoint), hints))
    return compiled


def _allows_none(tp: Any) -> bool:
    if tp is None or tp is type(None) or tp is Any:
        return True
    origin = get_origin(tp)
    if origin in (types.UnionType, Union):
        return any(a is type(None) for a in get_args(tp))
    return False


def _unwrap_optional(tp: Any) -> Any:
    origin = get_origin(tp)
    if origin in (types.UnionType, Union):
        non_none = [a for a in get_args(tp) if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return tp


def _coerce_query(raw: str, tp: Any) -> Any:
    base = _unwrap_optional(tp)
    if raw == "" and _allows_none(tp) and base in (int, float):
        return None
    if base is bool:
        return raw.strip().lower() in ("1", "true", "yes", "on")
    if base is int:
        return int(raw)
    if base is float:
        return float(raw)
    return raw


def _parse_multipart(content_type: str, body: bytes) -> tuple[dict[str, tuple[str, bytes]], dict[str, str]]:
    files: dict[str, tuple[str, bytes]] = {}
    fields: dict[str, str] = {}
    raw_msg = (
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("latin-1", errors="replace")
        + body
    )
    msg = email.parser.BytesParser(policy=email.policy.default).parsebytes(raw_msg)
    if not msg.is_multipart():
        return files, fields
    for part in msg.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if not name:
            continue
        payload = part.get_payload(decode=True) or b""
        filename = part.get_filename()
        if filename is not None:
            files[name] = (filename, payload)
        else:
            charset = part.get_content_charset() or "utf-8"
            fields[name] = payload.decode(charset, errors="replace")
    return files, fields


def _guess_mime(p: Path) -> str:
    ctype, _ = mimetypes.guess_type(str(p))
    if not ctype:
        ctype = "application/octet-stream"
    if ctype.startswith("text/") or ctype in ("application/javascript", "application/json"):
        return f"{ctype}; charset=utf-8"
    return ctype


def create_server(host: str = "0.0.0.0", port: int = 3000) -> ThreadingHTTPServer:
    for fn in getattr(main.app, "startup_handlers", []):
        fn()
    if not getattr(main.app, "startup_handlers", []):
        main.startup()

    routes = _compile_routes()
    mounts = list(getattr(main.app, "mounts", []))

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):
            pass

        def _send_bytes(self, status: int, data: bytes, ctype: str):
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _send_json(self, status: int, obj: Any):
            raw = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self._send_bytes(status, raw, "application/json; charset=utf-8")

        def _dispatch(self, method: str):
            try:
                parsed = urllib.parse.urlsplit(self.path)
                path = urllib.parse.unquote(parsed.path)

                # 1) 静态目录挂载 (/static/*)
                if method in ("GET", "HEAD"):
                    for prefix, sf in mounts:
                        if path.startswith(prefix + "/"):
                            rel = path[len(prefix) + 1 :]
                            target = (Path(sf.directory) / rel).resolve()
                            root = Path(sf.directory).resolve()
                            if (target == root or root in target.parents) and target.is_file():
                                data = target.read_bytes() if method == "GET" else b""
                                self._send_bytes(200, data, _guess_mime(target))
                                return

                # 2) 匹配路由
                matched = None
                path_allowed = False
                for methods, rx, endpoint, sig, hints in routes:
                    m = rx.match(path)
                    if m:
                        path_allowed = True
                        if method in methods or (method == "HEAD" and "GET" in methods):
                            matched = (m.groupdict(), endpoint, sig, hints)
                            break
                if matched is None:
                    self._send_json(405 if path_allowed else 404, {"detail": "Method Not Allowed" if path_allowed else "Not Found"})
                    return

                path_params, endpoint, sig, hints = matched
                query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

                content_len = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(content_len) if content_len > 0 else b""
                ctype = self.headers.get("Content-Type", "")

                files_map: dict[str, tuple[str, bytes]] = {}
                form_fields: dict[str, str] = {}
                json_body: dict[str, Any] | None = None
                if "multipart/form-data" in ctype.lower():
                    files_map, form_fields = _parse_multipart(ctype, body)
                elif body:
                    try:
                        json_body = json.loads(body.decode("utf-8"))
                    except Exception:
                        self._send_json(400, {"detail": "请求体 JSON 格式错误"})
                        return

                kwargs: dict[str, Any] = {}
                for pname, param in sig.parameters.items():
                    tp = hints.get(pname, param.annotation)
                    tp = Any if tp is inspect.Parameter.empty else tp
                    base_tp = _unwrap_optional(tp)
                    if pname in path_params:
                        kwargs[pname] = _coerce_query(path_params[pname], tp)
                    elif base_tp is main.UploadFile or getattr(base_tp, "__name__", "") == "UploadFile":
                        if pname not in files_map:
                            self._send_json(400, {"detail": f"缺少上传文件字段 {pname}"})
                            return
                        fname, fbytes = files_map[pname]
                        kwargs[pname] = main.UploadFile(filename=fname, content=fbytes)
                    elif isinstance(base_tp, type) and issubclass(base_tp, main.BaseModel):
                        data_dict = json_body if isinstance(json_body, dict) else {}
                        try:
                            kwargs[pname] = base_tp(**data_dict)
                        except main.HTTPException as e:
                            self._send_json(e.status_code, {"detail": e.detail})
                            return
                        except (ValueError, TypeError) as e:
                            self._send_json(422, {"detail": str(e)})
                            return
                    elif pname in query:
                        kwargs[pname] = _coerce_query(query[pname][-1], tp)
                    elif pname in form_fields:
                        kwargs[pname] = _coerce_query(form_fields[pname], tp)
                    elif param.default is not inspect.Parameter.empty:
                        kwargs[pname] = param.default
                    else:
                        self._send_json(422, {"detail": f"缺少必填参数: {pname}"})
                        return

                if inspect.iscoroutinefunction(endpoint):
                    res = asyncio.run(endpoint(**kwargs))
                else:
                    res = endpoint(**kwargs)

                if hasattr(res, "path"):
                    fpath = Path(res.path)
                    if not fpath.is_file():
                        self._send_json(404, {"detail": "File Not Found"})
                        return
                    data = fpath.read_bytes() if method != "HEAD" else b""
                    self._send_bytes(200, data, _guess_mime(fpath))
                else:
                    self._send_json(200, res)
            except main.HTTPException as e:
                self._send_json(e.status_code, {"detail": e.detail})
            except ValueError as e:
                self._send_json(400, {"detail": str(e)})
            except Exception as e:
                self._send_json(500, {"detail": str(e)})
            finally:
                db.close_conn()

        def do_GET(self):
            self._dispatch("GET")

        def do_HEAD(self):
            self._dispatch("HEAD")

        def do_POST(self):
            self._dispatch("POST")

        def do_DELETE(self):
            self._dispatch("DELETE")

    class ReusableServer(ThreadingHTTPServer):
        allow_reuse_address = True
        daemon_threads = True

    return ReusableServer((host, port), Handler)


def serve(host: str = "0.0.0.0", port: int = 3000):
    srv = create_server(host, port)

    def _shutdown(_signum, _frame):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    print(f"12306 智能中转服务已启动: http://{host}:{port}/", flush=True)
    try:
        srv.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        srv.server_close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="12306 智能中转纯 Python HTTP 服务")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=int(os.environ.get("DEFAULT_APP_PORT", "3000")))
    args = p.parse_args()
    serve(host=args.host, port=args.port)
