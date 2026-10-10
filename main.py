import argparse
import asyncio
import os
import hashlib
from cryptography.fernet import Fernet
from dataclasses import dataclass
from typing import List

@dataclass
class File:
    name: str
    size: int
    hash: str

class SyncNode:
    def __init__(self, port: int):
        self.port = port
        self.files: List[File] = []
        self.keys: dict = {}

    async def start(self):
        loop = asyncio.get_running_loop()
        server = await asyncio.start_server(self.handle_client, 'localhost', self.port)
        async with server:
            await server.serve_forever()

    async def handle_client(self, reader, writer):
        address = writer.get_extra_info('peername')
        print(f'Новое подключение: {address}')

        while True:
            data = await reader.read(1024)
            if not data:
                break

            if data.startswith(b'GET_FILES'):
                files = '\n'.join([f.name for f in self.files])
                writer.write(files.encode())
            elif data.startswith(b'GET_KEY'):
                key = Fernet.generate_key()
                self.keys[address] = key
                writer.write(key)
            elif data.startswith(b'UPLOAD_FILE'):
                file_name = data[13:].decode()
                file_size = os.path.getsize(file_name)
                file_hash = hashlib.sha256(open(file_name, 'rb').read()).hexdigest()
                self.files.append(File(file_name, file_size, file_hash))
                writer.write(f'FILE_UPLOADED {file_name} {file_hash}'.encode())

            await writer.drain()

        print(f'Клиент {address} отключился')
        writer.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8080)
    args = parser.parse_args()

    sync_node = SyncNode(args.port)
    asyncio.run(sync_node.start())