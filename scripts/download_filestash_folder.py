"""Recursively download a Filestash folder through its authenticated file API."""

from __future__ import annotations

import argparse
import getpass
import http.cookiejar
import json
import os
import posixpath
import re
import ssl
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlencode, urlsplit
from urllib.request import HTTPCookieProcessor, HTTPSHandler, Request, build_opener

try:
    from truststore import SSLContext as NativeSSLContext
except ModuleNotFoundError:
    NativeSSLContext = None


class FilestashError(RuntimeError):
    """Raised when Filestash returns an invalid or unsuccessful response."""


@dataclass(frozen=True)
class RemoteFile:
    remote_path: str
    relative_path: PurePosixPath
    size: int | None


@dataclass(frozen=True)
class DownloadResult:
    relative_path: PurePosixPath
    size: int
    status: str


def parse_frontend_url(url: str) -> tuple[str, str]:
    """Return the Filestash origin and remote path from a `/files/...` URL."""
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("The Filestash URL must include an http(s) scheme and hostname.")

    marker = "/files/"
    if not parsed.path.startswith(marker):
        raise ValueError("Expected a Filestash frontend URL whose path starts with /files/.")

    remote_path = "/" + unquote(parsed.path[len(marker) :]).lstrip("/")
    return f"{parsed.scheme}://{parsed.netloc}", normalize_directory_path(remote_path)


def normalize_directory_path(path: str) -> str:
    """Normalize a remote directory path without allowing parent traversal."""
    decoded = unquote(path).replace("\\", "/")
    parts = [part for part in decoded.split("/") if part not in {"", "."}]
    if ".." in parts:
        raise ValueError("Remote paths may not contain '..'.")
    normalized = "/" + "/".join(parts)
    return normalized.rstrip("/") + "/"


def safe_entry_name(name: str) -> str:
    """Validate that an API result is a single safe path component."""
    decoded = unquote(str(name or "")).replace("\\", "/").rstrip("/")
    if not decoded or decoded in {".", ".."} or "/" in decoded:
        raise FilestashError(f"Unsafe file entry returned by Filestash: {name!r}")
    return decoded


def native_ssl_context(ca_bundle: Path | None = None) -> ssl.SSLContext:
    """Create a verified TLS context using an explicit bundle or the native trust store."""
    if ca_bundle:
        return ssl.create_default_context(cafile=str(ca_bundle))
    if NativeSSLContext is not None:
        return NativeSSLContext(ssl.PROTOCOL_TLS_CLIENT)
    return ssl.create_default_context()


def bearer_token_from_cookies(cookies: http.cookiejar.CookieJar) -> str | None:
    """Reassemble Filestash's potentially split `auth`, `auth1`, ... cookies."""
    token_parts: dict[int, str] = {}
    for cookie in cookies:
        match = re.fullmatch(r"auth(\d*)", cookie.name)
        if match is None:
            continue
        token_parts[int(match.group(1) or 0)] = cookie.value

    if 0 not in token_parts:
        return None
    expected_indices = set(range(max(token_parts) + 1))
    if set(token_parts) != expected_indices:
        raise FilestashError("Filestash returned an incomplete split authentication cookie.")
    return "".join(token_parts[index] for index in sorted(token_parts))


def authenticate_with_password(
    base_url: str,
    username: str,
    password: str,
    *,
    connection: str,
    timeout: float = 60.0,
    ca_bundle: Path | None = None,
) -> str:
    """Log in through Filestash's user/password middleware and return its Bearer token."""
    if not username.strip() or not password:
        raise ValueError("Filestash username and password may not be empty.")
    if not connection.strip():
        raise ValueError("The Filestash connection label may not be empty.")

    origin = base_url.rstrip("/")
    cookies = http.cookiejar.CookieJar()
    opener = build_opener(
        HTTPSHandler(context=native_ssl_context(ca_bundle)),
        HTTPCookieProcessor(cookies),
    )
    login_url = (
        f"{origin}/api/session/auth/?"
        f"{urlencode({'action': 'redirect', 'label': connection.strip()})}"
    )
    body = urlencode({"user": username.strip(), "password": password}).encode()
    login_request = Request(
        login_url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        # Opening the form first is significant: Filestash stores the selected
        # connection in its short-lived `ssoref` cookie before accepting the POST.
        initial_request = Request(login_url, headers={"Accept": "text/html"})
        with opener.open(initial_request, timeout=timeout) as response:
            response.read()

        with opener.open(login_request, timeout=timeout) as response:
            final_url = response.geturl()
            response_body = response.read().decode("utf-8", errors="replace")

        if "Invalid username or password" in response_body:
            raise FilestashError(
                "Filestash's login form rejected the username or password for connection "
                f"{connection!r}."
            )

        token = bearer_token_from_cookies(cookies)
        if token:
            return token

        final_query = parse_qs(urlsplit(final_url).query)
        if final_query.get("error"):
            error = final_query["error"][0]
            trace = final_query.get("trace", [""])[0]
            detail = f": {trace}" if trace else ""
            raise FilestashError(
                "Filestash accepted the login form but could not open connection "
                f"{connection!r} ({error}){detail}"
            )

        # Fallback for configurations that expose the token through SessionGet.
        session_request = Request(
            f"{origin}/api/session",
            headers={"Accept": "application/json"},
        )
        with opener.open(session_request, timeout=timeout) as response:
            payload = json.load(response)
    except FilestashError:
        raise
    except HTTPError as exc:
        raise FilestashError(
            "Filestash did not establish an authenticated session for connection "
            f"{connection!r} (HTTP {exc.code})."
        ) from exc
    except URLError as exc:
        raise FilestashError(f"Could not reach Filestash: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise FilestashError("Filestash returned an invalid authentication response.") from exc

    result = payload.get("result") if isinstance(payload, dict) else None
    token = result.get("authorization") if isinstance(result, dict) else None
    if not isinstance(token, str) or not token:
        raise FilestashError(
            "Filestash login completed but did not return an access token for connection "
            f"{connection!r}."
        )
    return token


class FilestashClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = 60.0,
        ca_bundle: Path | None = None,
    ) -> None:
        if not token.strip():
            raise ValueError("A non-empty Filestash access token is required.")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.authorization = f"Bearer {token.strip()}"
        self.opener = build_opener(
            HTTPSHandler(context=native_ssl_context(ca_bundle)),
        )

    def _url(self, endpoint: str, **parameters: str) -> str:
        return f"{self.base_url}{endpoint}?{urlencode(parameters)}"

    def _open(self, request: Request):
        try:
            return self.opener.open(request, timeout=self.timeout)
        except HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8", errors="replace")
            except Exception:
                detail = ""
            if exc.code in {401, 403}:
                raise FilestashError(
                    "Filestash rejected the access token. Copy the Bearer token from an "
                    "authenticated browser request and try again."
                ) from exc
            raise FilestashError(
                f"Filestash returned HTTP {exc.code} for {request.full_url}: {detail[:500]}"
            ) from exc
        except URLError as exc:
            raise FilestashError(f"Could not reach Filestash: {exc.reason}") from exc

    def list_directory(self, remote_path: str) -> list[dict]:
        directory = normalize_directory_path(remote_path)
        request = Request(
            self._url("/api/files/ls", path=directory),
            headers={"Authorization": self.authorization, "Accept": "application/json"},
        )
        with self._open(request) as response:
            payload = json.load(response)
        if payload.get("status") != "ok" or not isinstance(payload.get("results"), list):
            raise FilestashError(f"Invalid directory response for {directory}: {payload!r}")
        return payload["results"]

    def open_file(self, remote_path: str, *, offset: int = 0):
        headers = {"Authorization": self.authorization}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        request = Request(
            self._url("/api/files/cat", path=remote_path),
            headers=headers,
        )
        return self._open(request)


def discover_files(
    client: FilestashClient,
    remote_root: str,
    *,
    extensions: set[str] | None = None,
) -> list[RemoteFile]:
    """Recursively list files below a remote directory."""
    root = normalize_directory_path(remote_root)
    pending: list[tuple[str, PurePosixPath]] = [(root, PurePosixPath())]
    visited: set[str] = set()
    discovered: list[RemoteFile] = []

    while pending:
        remote_directory, relative_directory = pending.pop()
        if remote_directory in visited:
            continue
        visited.add(remote_directory)

        for entry in client.list_directory(remote_directory):
            name = safe_entry_name(entry.get("name", ""))
            entry_type = str(entry.get("type") or "").lower()
            remote_child = posixpath.join(remote_directory.rstrip("/"), name)
            relative_child = relative_directory / name

            if entry_type == "file":
                suffix = Path(name).suffix.lower()
                if extensions and suffix not in extensions:
                    continue
                raw_size = entry.get("size")
                size = int(raw_size) if raw_size is not None else None
                discovered.append(
                    RemoteFile(
                        remote_path=remote_child,
                        relative_path=relative_child,
                        size=size,
                    )
                )
            elif entry_type in {"directory", "dir", "folder"}:
                pending.append((normalize_directory_path(remote_child), relative_child))
            else:
                print(
                    f"Skipping unsupported Filestash entry type {entry_type!r}: {remote_child}",
                    file=sys.stderr,
                )

    return sorted(discovered, key=lambda item: item.relative_path.as_posix())


def local_target(output_root: Path, relative_path: PurePosixPath) -> Path:
    """Map a safe relative POSIX path below an output directory."""
    if relative_path.is_absolute() or any(part in {"", ".", ".."} for part in relative_path.parts):
        raise FilestashError(f"Unsafe relative output path: {relative_path}")
    root = output_root.resolve()
    target = root.joinpath(*relative_path.parts).resolve()
    if target != root and root not in target.parents:
        raise FilestashError(f"Output path escapes the destination directory: {relative_path}")
    return target


def copy_stream(source: BinaryIO, destination: BinaryIO, chunk_size: int = 1024 * 1024) -> int:
    copied = 0
    while True:
        chunk = source.read(chunk_size)
        if not chunk:
            return copied
        destination.write(chunk)
        copied += len(chunk)


def download_one(
    client: FilestashClient,
    remote_file: RemoteFile,
    output_root: Path,
    *,
    retries: int,
) -> DownloadResult:
    target = local_target(output_root, remote_file.relative_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.is_file() and (remote_file.size is None or target.stat().st_size == remote_file.size):
        return DownloadResult(remote_file.relative_path, target.stat().st_size, "skipped")

    partial = target.with_name(f"{target.name}.part")
    for attempt in range(retries + 1):
        try:
            offset = partial.stat().st_size if partial.is_file() else 0
            if remote_file.size is not None and offset > remote_file.size:
                partial.unlink()
                offset = 0

            with client.open_file(remote_file.remote_path, offset=offset) as response:
                status = getattr(response, "status", response.getcode())
                append = offset > 0 and status == 206
                mode = "ab" if append else "wb"
                with partial.open(mode) as handle:
                    copy_stream(response, handle)

            downloaded_size = partial.stat().st_size
            if remote_file.size is not None and downloaded_size != remote_file.size:
                raise FilestashError(
                    f"Size mismatch for {remote_file.remote_path}: expected "
                    f"{remote_file.size}, downloaded {downloaded_size}"
                )
            partial.replace(target)
            return DownloadResult(remote_file.relative_path, downloaded_size, "downloaded")
        except FilestashError:
            if attempt >= retries:
                raise
            time.sleep(min(2**attempt, 8))

    raise AssertionError("Unreachable retry state")


def parse_extensions(values: list[str] | None) -> set[str] | None:
    if not values:
        return None
    return {value.lower() if value.startswith(".") else f".{value.lower()}" for value in values}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Recursively download an authenticated Filestash folder.",
    )
    parser.add_argument(
        "--url", required=True, help="Filestash browser URL beginning with /files/."
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument(
        "--username",
        help="Filestash username. When provided, the password is requested with a hidden prompt.",
    )
    parser.add_argument(
        "--connection",
        default="cellar-edito",
        help="Filestash connection label used for username/password login (default: cellar-edito).",
    )
    parser.add_argument(
        "--password-env",
        default="FILESTASH_PASSWORD",
        help="Optional environment variable containing the password (default: FILESTASH_PASSWORD).",
    )
    parser.add_argument(
        "--token-env",
        default="FILESTASH_TOKEN",
        help="Bearer-token environment variable used when --username is omitted.",
    )
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--ca-bundle", type=Path)
    parser.add_argument(
        "--extensions",
        nargs="+",
        help="Optional extensions to include, for example: png jpg webp.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.workers < 1:
        raise SystemExit("--workers must be at least 1")
    if args.retries < 0:
        raise SystemExit("--retries may not be negative")

    base_url, remote_root = parse_frontend_url(args.url)
    if args.username:
        password = os.environ.get(args.password_env) or getpass.getpass("Filestash password: ")
        token = authenticate_with_password(
            base_url,
            args.username,
            password,
            connection=args.connection,
            timeout=args.timeout,
            ca_bundle=args.ca_bundle,
        )
    else:
        token = os.environ.get(args.token_env) or getpass.getpass("Filestash Bearer token: ")
    client = FilestashClient(
        base_url,
        token,
        timeout=args.timeout,
        ca_bundle=args.ca_bundle,
    )
    files = discover_files(client, remote_root, extensions=parse_extensions(args.extensions))
    total_size = sum(item.size or 0 for item in files)
    print(f"Found {len(files):,} files ({total_size:,} declared bytes) under {remote_root}")

    if args.dry_run:
        for item in files:
            print(f"{item.relative_path.as_posix()}\t{item.size if item.size is not None else '?'}")
        return 0

    output_root = args.output_dir.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                download_one,
                client,
                remote_file,
                output_root,
                retries=args.retries,
            ): remote_file
            for remote_file in files
        }
        for future in as_completed(futures):
            remote_file = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                raise FilestashError(
                    f"Download failed for {remote_file.remote_path}: {exc}"
                ) from exc
            results.append(result)
            print(f"{result.status:10} {result.relative_path.as_posix()} ({result.size:,} bytes)")

    downloaded = sum(result.status == "downloaded" for result in results)
    skipped = sum(result.status == "skipped" for result in results)
    print(f"Complete: {downloaded:,} downloaded, {skipped:,} already present.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FilestashError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
