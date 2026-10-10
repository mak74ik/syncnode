import asyncio
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from main import SyncNodeClient, SyncNodeServer


class TestSyncNode(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_storage = tempfile.mkdtemp()
        self.port = 18088
        self.server_obj = SyncNodeServer(host="127.0.0.1", port=self.port, storage_dir=self.temp_storage)
        self.server = await asyncio.start_server(self.server_obj.handle_client, "127.0.0.1", self.port)
        self.client = SyncNodeClient(host="127.0.0.1", port=self.port)

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()
        shutil.rmtree(self.temp_storage, ignore_errors=True)

    async def test_upload_list_and_download(self):
        # 1. Create a dummy local file
        with tempfile.NamedTemporaryFile("wb", delete=False) as f:
            f.write(b"SyncNode verified test content 123456789")
            dummy_path = f.name

        try:
            # 2. Upload to server
            ok = await self.client.upload_file(dummy_path)
            self.assertTrue(ok)

            # 3. List files on server
            files = await self.client.list_files()
            fname = os.path.basename(dummy_path)
            self.assertTrue(any(item["name"] == fname for item in files))

            # 4. Download file back
            down_path = dummy_path + ".downloaded"
            ok_down = await self.client.download_file(fname, down_path)
            self.assertTrue(ok_down)

            with open(down_path, "rb") as f:
                content = f.read()
            self.assertEqual(content, b"SyncNode verified test content 123456789")
            if os.path.exists(down_path):
                os.remove(down_path)

        finally:
            if os.path.exists(dummy_path):
                os.remove(dummy_path)

    async def test_path_traversal_blocked(self):
        # Attempt to upload with path traversal in name
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        writer.write(b"UPLOAD ../../../malicious.txt 10\n")
        await writer.drain()
        resp = await reader.readline()
        self.assertTrue(resp.startswith(b"ERR"))
        writer.close()
        await writer.wait_closed()


if __name__ == "__main__":
    unittest.main()
