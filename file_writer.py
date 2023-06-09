import asyncio
import os

import aiofiles


class FileWriter:
    def __init__(self, file_path, file_size, pbar, buffer_queue: asyncio.Queue):
        self.file_path = file_path
        self.file_size = file_size
        self.tmp_file = file_path + '.tmp'
        self.buffer_queue = buffer_queue
        self.pbar = pbar

    async def start_consuming(self):
        async with aiofiles.open(self.tmp_file, 'wb') as f:
            print(f"预分配文件空间 {self.file_size}B")
            await f.truncate(self.file_size)

            while True:
                data = await self.buffer_queue.get()
                if data is None:
                    break
                await f.seek(data[0])
                await f.write(data[1])
                self.pbar.update(len(data[1]))
                self.buffer_queue.task_done()
        print("文件写入者退出")
        os.rename(self.tmp_file, self.file_path)


class TestFileWriter(FileWriter):

    async def start_consuming(self):
        async with aiofiles.open(self.tmp_file, 'wb') as f:
            lll = 0
            while True:
                data = await self.buffer_queue.get()
                if data is None:
                    break
                await f.write(data[1])
                # self.pbar.update(len(data[1]))
                print("current_size",lll := lll + len(data[1]))
                # self.buffer_queue.task_done()
            await f.close()
        print("文件写入者退出")
        # os.path.getsize(f"file size:{os.path.getsize(self.tmp_file)})")
        # os.rename(self.tmp_file, self.file_path)
