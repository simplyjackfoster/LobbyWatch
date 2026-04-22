# cli/lobbywatch/commands/update.py
import os
from pathlib import Path

import httpx
import zstandard

DEFAULT_URL = "https://github.com/simplyjackfoster/LobbyWatch/releases/latest/download/lobbywatch.db.zst"


def download_and_install(url: str, db_path: str) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    tmp = db_path + ".tmp"
    dctx = zstandard.ZstdDecompressor()
    if url.startswith("file://"):
        src_path = url[7:]
        with open(src_path, "rb") as src, open(tmp, "wb") as dst:
            dctx.copy_stream(src, dst)
    else:
        with httpx.Client(follow_redirects=True, timeout=300) as client:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as dst:
                    writer = dctx.stream_writer(dst)
                    for chunk in resp.iter_bytes(chunk_size=1024 * 1024):
                        writer.write(chunk)
                    writer.close()
    os.replace(tmp, db_path)
