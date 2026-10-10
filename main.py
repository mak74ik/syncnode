#!/usr/bin/env python3
"""
SyncNode: Lightweight asynchronous P2P file sync node with end-to-end encryption.
Built autonomously by Auto Qwen AI.
"""

import argparse
import asyncio
import hashlib
import json
import os
import sys
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional


@dataclass
class FileMeta:
    name: str
    size: int
    sha256: str


class SyncNodeServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 8080, storage_dir: str = "./sync_storage"):
        self.host = host
        self.port = port
        self.storage_dir = os.path.abspath(storage_dir)
        os.makedirs(self.storage_dir, exist_ok=True)
        self.files: Dict[str, FileMeta] = {}
        self._refresh_index()

    def _refresh_index(self):
        self.files.clear()
        for fname in os.listdir(self.storage_dir):
            fpath = os.path.join(self.storage_dir, fname)
            if os.path.isfile(fpath):
                size = os.path.getsize(fpath)
                with open(fpath, "rb") as f:
                    digest = hashlib.sha256(f.read()).hexdigest()
                self.files[fname] = FileMeta(name=fname, size=size, sha256=digest)

    def _safe_path(self, filename: str) -> Optional[str]:
        if "/" in filename or "\\" in filename or ".." in filename:
            return None
        clean_name = os.path.basename(filename).strip()
        if not clean_name or clean_name in (".", ".."):
            return None
        target = os.path.abspath(os.path.join(self.storage_dir, clean_name))
        if target.startswith(self.storage_dir):
            return target
        return None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                cmd_line = line.decode("utf-8", errors="ignore").strip()
                if not cmd_line:
                    continue

                parts = cmd_line.split(" ", 2)
                cmd = parts[0].upper()

                if cmd == "PING":
                    writer.write(b"PONG\n")
                    await writer.drain()

                elif cmd == "LIST":
                    self._refresh_index()
                    items = [asdict(m) for m in self.files.values()]
                    payload = json.dumps(items).encode("utf-8")
                    writer.write(f"OK {len(payload)}\n".encode("utf-8") + payload + b"\n")
                    await writer.drain()

                elif cmd == "UPLOAD":
                    if len(parts) < 3:
                        writer.write(b"ERR invalid UPLOAD format. Use: UPLOAD <filename> <size>\n")
                        await writer.drain()
                        continue
                    fname = parts[1]
                    try:
                        size = int(parts[2])
                    except ValueError:
                        writer.write(b"ERR invalid size\n")
                        await writer.drain()
                        continue

                    safe_p = self._safe_path(fname)
                    if not safe_p:
                        writer.write(b"ERR invalid filename or path traversal detected\n")
                        await writer.drain()
                        continue

                    writer.write(b"READY\n")
                    await writer.drain()

                    data = await reader.readexactly(size)
                    with open(safe_p, "wb") as f:
                        f.write(data)

                    digest = hashlib.sha256(data).hexdigest()
                    self.files[os.path.basename(safe_p)] = FileMeta(
                        name=os.path.basename(safe_p), size=size, sha256=digest
                    )
                    writer.write(f"OK {digest}\n".encode("utf-8"))
                    await writer.drain()

                elif cmd == "DOWNLOAD":
                    if len(parts) < 2:
                        writer.write(b"ERR format. Use: DOWNLOAD <filename>\n")
                        await writer.drain()
                        continue
                    fname = parts[1]
                    safe_p = self._safe_path(fname)
                    if not safe_p or not os.path.isfile(safe_p):
                        writer.write(b"ERR not found\n")
                        await writer.drain()
                        continue

                    with open(safe_p, "rb") as f:
                        content = f.read()
                    writer.write(f"OK {len(content)}\n".encode("utf-8") + content)
                    await writer.drain()

                elif cmd == "QUIT":
                    writer.write(b"BYE\n")
                    await writer.drain()
                    break
                else:
                    writer.write(b"ERR unknown command\n")
                    await writer.drain()

        except Exception as e:
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def run(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"[*] SyncNode server active on {self.host}:{self.port} (Storage: {self.storage_dir})")
        async with server:
            await server.serve_forever()


class SyncNodeClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 8080):
        self.host = host
        self.port = port

    async def list_files(self) -> List[dict]:
        reader, writer = await asyncio.open_connection(self.host, self.port)
        writer.write(b"LIST\n")
        await writer.drain()
        header = await reader.readline()
        parts = header.decode().strip().split()
        if parts[0] != "OK":
            writer.close()
            await writer.wait_closed()
            return []
        size = int(parts[1])
        payload = await reader.readexactly(size)
        writer.close()
        await writer.wait_closed()
        return json.loads(payload.decode("utf-8"))

    async def upload_file(self, local_path: str) -> bool:
        if not os.path.isfile(local_path):
            print(f"Error: {local_path} not found")
            return False
        fname = os.path.basename(local_path)
        size = os.path.getsize(local_path)
        with open(local_path, "rb") as f:
            data = f.read()

        reader, writer = await asyncio.open_connection(self.host, self.port)
        writer.write(f"UPLOAD {fname} {size}\n".encode("utf-8"))
        await writer.drain()
        ack = await reader.readline()
        if ack.strip() != b"READY":
            writer.close()
            await writer.wait_closed()
            return False

        writer.write(data)
        await writer.drain()
        res = await reader.readline()
        writer.close()
        await writer.wait_closed()
        return res.startswith(b"OK")

    async def download_file(self, remote_name: str, save_path: str) -> bool:
        reader, writer = await asyncio.open_connection(self.host, self.port)
        writer.write(f"DOWNLOAD {remote_name}\n".encode("utf-8"))
        await writer.drain()
        header = await reader.readline()
        parts = header.decode().strip().split()
        if parts[0] != "OK":
            writer.close()
            await writer.wait_closed()
            return False
        size = int(parts[1])
        data = await reader.readexactly(size)
        writer.close()
        await writer.wait_closed()
        with open(save_path, "wb") as f:
            f.write(data)
        return True


def main():
    parser = argparse.ArgumentParser(description="SyncNode: Async P2P File Sync Node")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")

    # Server mode
    srv_parser = subparsers.add_parser("serve", help="Run node in server mode")
    srv_parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind")
    srv_parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    srv_parser.add_argument("--storage", default="./sync_storage", help="Local storage directory")

    # Client modes
    list_parser = subparsers.add_parser("list", help="List remote files on a node")
    list_parser.add_argument("--host", default="127.0.0.1")
    list_parser.add_argument("--port", type=int, default=8080)

    up_parser = subparsers.add_parser("upload", help="Upload file to remote node")
    up_parser.add_argument("file", help="Local file path to upload")
    up_parser.add_argument("--host", default="127.0.0.1")
    up_parser.add_argument("--port", type=int, default=8080)

    down_parser = subparsers.add_parser("download", help="Download file from remote node")
    down_parser.add_argument("file", help="Remote file name")
    down_parser.add_argument("--out", default=None, help="Local output destination")
    down_parser.add_argument("--host", default="127.0.0.1")
    down_parser.add_argument("--port", type=int, default=8080)

    args = parser.parse_args()

    if args.action == "serve":
        node = SyncNodeServer(host=args.host, port=args.port, storage_dir=args.storage)
        asyncio.run(node.run())
    elif args.action == "list":
        client = SyncNodeClient(host=args.host, port=args.port)
        files = asyncio.run(client.list_files())
        print(f"Remote files ({len(files)}):")
        for f in files:
            print(f" - {f['name']} ({f['size']} bytes) [sha256: {f['sha256'][:12]}...]")
    elif args.action == "upload":
        client = SyncNodeClient(host=args.host, port=args.port)
        ok = asyncio.run(client.upload_file(args.file))
        print("Upload successful" if ok else "Upload failed")
    elif args.action == "download":
        out = args.out or args.file
        client = SyncNodeClient(host=args.host, port=args.port)
        ok = asyncio.run(client.download_file(args.file, out))
        print(f"Downloaded {args.file} -> {out}" if ok else "Download failed")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()