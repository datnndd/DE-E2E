from __future__ import annotations

import hashlib
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib import request


TEST_BYTES = (b"douyin-media-test-" * 4096) + b"done"


class RangeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        range_header = self.headers.get("Range")
        start = 0
        if range_header and range_header.startswith("bytes="):
            start = int(range_header.removeprefix("bytes=").split("-", 1)[0] or "0")
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{len(TEST_BYTES) - 1}/{len(TEST_BYTES)}")
        else:
            self.send_response(200)
        body = TEST_BYTES[start:]
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        for index in range(0, len(body), 1024):
            self.wfile.write(body[index : index + 1024])

    def log_message(self, *_):
        return


def download_url_to_file(url: str, file_path: Path, chunk_size: int = 8192) -> tuple[str | None, str]:
    existing_size = file_path.stat().st_size if file_path.exists() else 0
    headers = {"Range": f"bytes={existing_size}-"} if existing_size > 0 else {}
    req = request.Request(url, headers=headers, method="GET")
    with request.urlopen(req, timeout=10) as response:
        status_code = getattr(response, "status", response.getcode())
        if status_code not in {200, 206}:
            raise RuntimeError(f"HTTP {status_code}")
        mode = "ab" if existing_size > 0 and status_code == 206 else "wb"
        expected_remaining = int(response.headers.get("Content-Length") or 0)
        bytes_written = 0
        with file_path.open(mode) as file:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                file.write(chunk)
                bytes_written += len(chunk)
        if expected_remaining and bytes_written < expected_remaining:
            raise RuntimeError(f"IncompleteRead({bytes_written} bytes read, {expected_remaining - bytes_written} more expected)")
        return response.headers.get("Content-Type"), url


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/media.bin"
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            full_path = Path(tmpdir) / "full.bin"
            content_type, _ = download_url_to_file(url, full_path)
            assert content_type == "application/octet-stream"
            assert full_path.read_bytes() == TEST_BYTES

            resume_path = Path(tmpdir) / "resume.bin"
            resume_path.write_bytes(TEST_BYTES[:12345])
            download_url_to_file(url, resume_path)
            assert resume_path.read_bytes() == TEST_BYTES

            print("media stream download test OK")
            print(f"bytes={len(TEST_BYTES)} sha256={sha256(TEST_BYTES)}")
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
