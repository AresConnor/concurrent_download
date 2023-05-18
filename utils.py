import os

from tqdm.asyncio import tqdm


def need_redirection(response):
    if response.status == 302:
        return True
    else:
        return False


def check_if_support_breakpoint(headers):
    if headers.get('Accept-Ranges') == 'bytes':
        print("支持断点续传")
        return True
    else:
        print("不支持断点续传")
        return False


async def check_file(output_path, file_size):
    start_byte = 0
    if os.path.exists(output_path):
        local_file_size = os.path.getsize(output_path)
        if local_file_size < file_size:
            start_byte = local_file_size
            return False, start_byte
        elif local_file_size == file_size:
            print("文件已完整下载")
            return True, file_size
        else:
            os.remove(output_path)
            return False, 0
    else:
        print("本地文件不存在，开始下载")
        return False, 0


def merge_file(output_path, fn, total_size, thread):
    # 合并文件,进度条，单位是字节
    with tqdm(total=total_size, unit='B', unit_scale=True, desc="合并文件") as pbar:
        with open(f"{output_path}/{fn}", "wb") as f:
            for i in range(thread):
                part_file_name = f"{output_path}/{fn}.part{i}"
                with open(part_file_name, "rb") as part_f:
                    f.write(part_f.read())
                    pbar.update(os.path.getsize(part_file_name))
                os.remove(part_file_name)


def print_method_name(func):
    def wrapper(*args, **kwargs):
        class_name = args[0].__class__.__name__
        method_name = func.__name__
        print(f"Class: {class_name}, Method: {method_name}")
        return func(*args, **kwargs)
    return wrapper


def print_func_name(f):
    name = f.__name__

    def new_f(*a, **ka):
        print(f"func name = {name}")
        return f(*a, **ka)

    return new_f
