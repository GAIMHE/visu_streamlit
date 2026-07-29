from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.cookiejar import Cookie, CookieJar
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, parse_qsl, urlsplit

import pytest

from scripts.download_filestash_folder import (
    FilestashClient,
    FilestashError,
    authenticate_with_password,
    bearer_token_from_cookies,
    discover_files,
    download_one,
    parse_frontend_url,
    safe_entry_name,
)

FILES = {
    "/smith-bucket/adaptivcollege/root.png": b"root-image",
    "/smith-bucket/adaptivcollege/nested/child.jpg": b"child-image-data",
}


class FilestashHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        parsed = urlsplit(self.path)
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode()
        form = dict(parse_qsl(body))
        query = parse_qs(parsed.query)

        if (
            parsed.path == "/api/session/auth/"
            and query.get("label") == ["cellar-edito"]
            and form == {"user": "test-user", "password": "test-password"}
            and "ssoref=cellar-edito" in self.headers.get("Cookie", "")
        ):
            self.send_response(303)
            self.send_header("Location", "/")
            self.send_header("Set-Cookie", "auth=test-; Path=/; HttpOnly")
            self.send_header("Set-Cookie", "auth1=token; Path=/; HttpOnly")
            self.end_headers()
            return

        self.send_response(303)
        self.send_header("Location", "/api/session/auth/?action=redirect")
        self.send_header("Set-Cookie", 'flash="Invalid username or password"; Path=/')
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        if parsed.path == "/api/session/auth/":
            invalid = "flash=" in self.headers.get("Cookie", "")
            body = b"<p>Invalid username or password</p>" if invalid else b"<form>Login</form>"
            self.send_response(200)
            if parse_qs(parsed.query).get("label") == ["cellar-edito"]:
                self.send_header("Set-Cookie", "ssoref=cellar-edito; Path=/")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/":
            body = b"<html>Filestash</html>"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/api/session":
            self.send_response(401)
            self.end_headers()
            return

        token = self.headers.get("Authorization")
        if token != "Bearer test-token":
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"error","message":"Not authorised"}')
            return

        path = parse_qs(parsed.query).get("path", [""])[0]
        if parsed.path == "/api/files/ls":
            results = {
                "/smith-bucket/adaptivcollege/": [
                    {
                        "name": "root.png",
                        "type": "file",
                        "size": len(FILES["/smith-bucket/adaptivcollege/root.png"]),
                    },
                    {"name": "nested", "type": "directory", "size": 0},
                ],
                "/smith-bucket/adaptivcollege/nested/": [
                    {
                        "name": "child.jpg",
                        "type": "file",
                        "size": len(FILES["/smith-bucket/adaptivcollege/nested/child.jpg"]),
                    }
                ],
            }[path]
            body = json.dumps({"status": "ok", "results": results}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/api/files/cat":
            body = FILES[path]
            range_header = self.headers.get("Range")
            if range_header:
                offset = int(range_header.removeprefix("bytes=").removesuffix("-"))
                body = body[offset:]
                self.send_response(206)
            else:
                self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


@contextmanager
def filestash_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FilestashHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_parse_frontend_url() -> None:
    base_url, remote_path = parse_frontend_url(
        "https://example.test/files/smith-bucket/adaptivcollege/"
    )
    assert base_url == "https://example.test"
    assert remote_path == "/smith-bucket/adaptivcollege/"


@pytest.mark.parametrize("name", ["", ".", "..", "../escape.png", "nested/file.png"])
def test_safe_entry_name_rejects_unsafe_values(name: str) -> None:
    with pytest.raises(FilestashError):
        safe_entry_name(name)


def test_recursive_discovery_resume_and_skip(tmp_path: Path) -> None:
    with filestash_server() as base_url:
        client = FilestashClient(base_url, "test-token")
        files = discover_files(client, "/smith-bucket/adaptivcollege/")

        assert [item.relative_path.as_posix() for item in files] == [
            "nested/child.jpg",
            "root.png",
        ]

        child = files[0]
        child_target = tmp_path / "nested" / "child.jpg"
        child_target.parent.mkdir(parents=True)
        child_target.with_name("child.jpg.part").write_bytes(FILES[child.remote_path][:5])

        result = download_one(client, child, tmp_path, retries=0)
        assert result.status == "downloaded"
        assert child_target.read_bytes() == FILES[child.remote_path]
        assert not child_target.with_name("child.jpg.part").exists()

        skipped = download_one(client, child, tmp_path, retries=0)
        assert skipped.status == "skipped"


def test_password_authentication_returns_bearer_token() -> None:
    with filestash_server() as base_url:
        token = authenticate_with_password(
            base_url,
            "test-user",
            "test-password",
            connection="cellar-edito",
        )
    assert token == "test-token"


def test_bearer_token_from_cookies_reassembles_split_token() -> None:
    cookies = CookieJar()
    for name, value in (("auth1", "token"), ("auth", "test-")):
        cookies.set_cookie(
            Cookie(
                version=0,
                name=name,
                value=value,
                port=None,
                port_specified=False,
                domain="example.test",
                domain_specified=True,
                domain_initial_dot=False,
                path="/",
                path_specified=True,
                secure=True,
                expires=None,
                discard=True,
                comment=None,
                comment_url=None,
                rest={},
                rfc2109=False,
            )
        )
    assert bearer_token_from_cookies(cookies) == "test-token"


def test_password_authentication_rejects_invalid_credentials() -> None:
    with filestash_server() as base_url:
        with pytest.raises(FilestashError, match="login form rejected"):
            authenticate_with_password(
                base_url,
                "wrong-user",
                "wrong-password",
                connection="cellar-edito",
            )


def test_client_reports_authentication_failure() -> None:
    with filestash_server() as base_url:
        client = FilestashClient(base_url, "wrong-token")
        with pytest.raises(FilestashError, match="rejected the access token"):
            client.list_directory("/smith-bucket/adaptivcollege/")
