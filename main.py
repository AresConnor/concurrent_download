import asyncio
import os
import sys
from typing import Tuple, Optional

import aiofiles
import aiohttp
from tqdm.asyncio import tqdm

from multi_downloader.download_manager import DownloadWorkerManager
from multi_downloader.utils import check_if_support_breakpoint, merge_file, check_file, need_redirection




async def mdownload_file_tqdm(url: str, output_path: str, thread: int, headers: Optional[dict],
                              file_name: str = "UNNAMED_FILE",
                              proxy='',
                              r_headers=...) -> Tuple[bool, str]:
    """
    download_file的带进度条版
    :param thread:
    :param proxy:
    :param file_name: 默认的文件名
    :param url:
    :param output_path: 通常是文件夹
    :param headers:额外的请求头，不确定有没有用
    :param r_headers:这是缓存的响应头，不要动这个
    :return:返回 1.是否正确下载 2.下载的文件的绝对地址
    """
    if headers is None:
        headers = {}
    file_size = 0
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.head(url, headers=headers) as response:
            if not response.ok:
                print(f"status error: {response.status}")
                return False, ''
            # 提取响应头
            r_headers = response.headers

            if need_redirection(response):
                url = r_headers.get('Location')
                print(f"重定向地址为 {url}")
                await session.close()
                return await mdownload_file_tqdm(url, output_path, thread, headers, file_name, proxy, r_headers)
            else:
                # 获取文件大小
                file_size = int(r_headers.get('Content-length', 0))
                print(f"文件大小为 {file_size / 1024 / 1024:.2f} MB")

                # 服务器返回的文件名
                fn = getattr(response.content_disposition, "filename", file_name)
                file_path = os.path.abspath(os.path.join(output_path, fn))
                print(f"获取到文件名: {fn}")

                if check_if_support_breakpoint(r_headers):
                    response.raise_for_status()
                    # 多线程下载
                    manager = DownloadWorkerManager(url=url, file_name=fn, file_size=file_size, workers=thread,
                                                    headers=headers, proxy=proxy)
                    await manager.start()
                    print("文件下载完成")
                    return True, file_path
                else:
                    print("不支持断点续传")
                    response.raise_for_status()
                    async with session.get(url, headers=headers, proxy=proxy, verify_ssl=False) as response_download:
                        response_download.raise_for_status()
                        async with aiofiles.open(file_path, 'wb') as f:
                            pbar = tqdm(total=file_size, initial=0, unit='B', unit_scale=True)
                            pbar.set_postfix({"FileName": fn})
                            bytes_downloaded = 0
                            async for b in response_download.content.iter_any():
                                await f.write(b)
                                bytes_downloaded += len(b)
                                pbar.update(len(b))
                            pbar.close()
                    print("文件下载完成")
                    return True, file_path



async def main():
    asyncio.get_event_loop().set_debug(True)
    url2 = 'https://api.github.com/repos/MRSlouzk/External_tianxi_renpy/releases/assets/107655503'
    url = 'https://github.com/Fndroid/clash_for_windows_pkg/releases/download/0.20.22/Clash.for.Windows-0.20.22-x64-linux.tar.gz'
    url1 = 'https://github.com/MRSlouzk/External_tianxi_renpy/releases/download/v1.0.1/reconstruct.toml'
    url3 = 'https://api.github.com/repos/MRSlouzk/External_tianxi_renpy/releases/assets/107215675'
    url4 = 'https://down.qq.com/qqweb/LinuxQQ/linuxqq_2.0.0-b2-1089_amd64.deb'
    url5 = 'https://dldir1.qq.com/qqfile/qq/PCQQ9.7.7/QQ9.7.7.29006.exe'
    print(await mdownload_file_tqdm(url4, '.', 2, file_name="test", proxy='', headers={}))


if __name__ == '__main__':
    asyncio.run(main())
