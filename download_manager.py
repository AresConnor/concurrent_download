import asyncio
import functools

from typing import List, Dict, Iterable

from aiohttp import ClientSession
from tqdm import tqdm

from download_worker import DownloadWorker
from events import WorkerBufferFullEvent, WorkerInactiveEvent, WorkerActiveEvent, \
    WorkerReceivedTaskEvent
from file_writer import FileWriter, TestFileWriter
from task import WorkerTask, WorkerTaskMaxHeap


class WorkerTaskHistory:
    """
    用于记录WorkerTask的下载历史，用于断点续传
    """

    # TODO

    def __init__(self):
        # queue
        self._history = []

    def append_history(self, task: WorkerTask):
        self._history.append(task)


class WorkerTasks:
    def __init__(self):
        # 使用大根堆来存储任务
        self.unfinished_tasks: Dict[WorkerTask.State, WorkerTaskMaxHeap] = {}
        self.unfinished_task_states: Dict[WorkerTask, WorkerTask.State] = {}
        for state in WorkerTask.State:
            if state != WorkerTask.State.FINISHED:
                self.unfinished_tasks[state] = WorkerTaskMaxHeap()
        self.history = WorkerTaskHistory()

    def add_task(self, task: WorkerTask):
        # 标记
        task.assign(-1)
        self.history.append_history(task)
        if not self.unfinished_tasks[task.get_state()].has(task):
            self.unfinished_tasks[task.get_state()].push(task)
            self.unfinished_task_states[task] = task.get_state()

    def add_tasks(self, tasks: Iterable[WorkerTask]):
        for task in tasks:
            self.add_task(task)

    def get_top_task(self, state: WorkerTask.State):
        task = self.unfinished_tasks[state].pop_max()
        return task

    def get_size(self, state: WorkerTask.State):
        return len(self.unfinished_tasks[state])

    def get_top_task_ref(self, state: WorkerTask.State):
        return self.unfinished_tasks[state].get_max()

    def get_new_tasks(self, state: WorkerTask.State, num: int):
        """
        该算法还不够完善，需要考虑是否平均分配的问题
        :param state: != FINISHED
        :param num:
        :return:
        """
        # TODO
        task = self.unfinished_tasks[state].get_max()
        new_tasks = task.try_divides(num)
        self.add_task(task)
        self.add_tasks(new_tasks)
        return new_tasks

    @functools.singledispatchmethod
    def update_task_state(self, task: WorkerTask) -> None:
        old_task_state = self.unfinished_task_states[task]
        new_task_state = task.get_state()
        if old_task_state == new_task_state:
            return
        # update state
        if task.get_state() != WorkerTask.State.FINISHED:
            self.unfinished_task_states[task] = new_task_state
        # update tasks
        self.unfinished_tasks[old_task_state].remove(task)
        if task.get_state() != WorkerTask.State.FINISHED:
            self.unfinished_tasks[new_task_state].push(task)
        # update history
        self.history.append_history(task)

    def update_task_states(self, tasks: Iterable[WorkerTask]) -> None:
        """
        更新alive_tasks
        :param tasks:
        :return:
        """
        for task in tasks:
            self.update_task_state(task)


class DownloadWorkerManager:

    def __init__(self, url, file_name, file_size, workers, headers=None, proxy=None,
                 task_retry_times=3):
        """
        负责管理多个worker，接收worker的事件，分配任务，动态调度
        :param url:
        :param file_name:
        :param file_size:
        :param workers:
        :param headers:
        :param proxy:
        :param task_retry_times:
        """
        self._url = url
        self._file_name = file_name
        self._file_size = file_size
        self._workers = workers
        self._progress_bar = None
        self._headers = headers
        self._proxy = proxy
        self._task_retry_times = task_retry_times

        self._workers_request_num = 0

        self._session = None
        self.future = None
        self._tasks = WorkerTasks()
        self.worker_instances = []
        self.buffer_queue = asyncio.Queue(maxsize=-1)
        self.pending_task_queue = asyncio.Queue(maxsize=-1)

        self.file_writer = None

    def try_divide_running_tasks(self, worker_num) -> List[WorkerTask]:
        """
        分配任务:
        :param task_state:
        :param worker_num:可用worker数目
        :return:
        """
        new_tasks = []
        if self._tasks.get_size(WorkerTask.State.RUNNING) > 0:
            for i in range(worker_num):
                task = self._tasks.get_top_task(WorkerTask.State.RUNNING)
                if (new_task := task.try_divide()) is not None:
                    new_tasks.append(new_task)
                    self._tasks.add_tasks([task, new_task])
                else:
                    break
        return new_tasks

    def try_divide_pending_tasks(self, worker_num) -> List[WorkerTask]:
        """
        分配任务:
        :param task_state:
        :param worker_num:可用worker数目
        :return:
        """
        new_tasks = []
        if self._tasks.get_size(WorkerTask.State.PENDING) > 0:
            for i in range(worker_num):
                task = self._tasks.get_top_task(WorkerTask.State.PENDING)
                if (new_task := task.try_divide()) is not None:
                    new_tasks.append(new_task)
                    self._tasks.add_tasks([task, new_task])
                else:
                    break
        return new_tasks

    def try_feed_waiting_workers_task(self, workers: int) -> List[WorkerTask]:
        """
        将任务分配给workers
        优先分配PENDING的任务,然后是ERROR的任务，最后是RUNNING的任务
        PENDING分配如下：
            如果该任务没有被分配:
                直接加入tasks队列-1
            如果任务已经被分配:
                将任务平均分割,加入tasks队列
        ERROR分配如下：
            直接加入tasks队列
            可用worker数目-1
        RUNNING分配如下：
            将任务平均分割，加入tasks队列
            可用worker数目-m
        :return:
        """
        new_tasks = []
        # 使用while 当做goto
        while True:
            # 优先分配PENDING的任务
            if self._tasks.get_size(WorkerTask.State.PENDING) > 0:
                # 直接分配
                for i in range(workers):
                    if self._tasks.get_size(WorkerTask.State.PENDING) > 0:

                        if not (task := self._tasks.get_top_task_ref(WorkerTask.State.PENDING)).is_assigned():
                            # 如果这个任务还没有被分配(没有被标记或者被worker认领):
                            self._tasks.get_top_task(WorkerTask.State.PENDING)
                            new_tasks.append(task)
                            workers -= 1
                        else:
                            # 如果已被分配(被标记或者被worker认领)
                            tasks = self.try_divide_pending_tasks(workers)
                            new_tasks.extend(tasks)
                            workers -= len(tasks)
                    else:
                        break
                break
            # 然后是ERROR的任务
            if self._tasks.get_size(WorkerTask.State.ERROR) > 0:
                # 直接分配
                for i in range(workers):
                    if self._tasks.get_size(WorkerTask.State.ERROR) > 0:
                        task = self._tasks.get_top_task(WorkerTask.State.ERROR)
                        new_tasks.append(task)
                        workers -= 1
                    else:
                        break
                break
            # 最后是RUNNING的任务
            if self._tasks.get_size(WorkerTask.State.RUNNING) > 0:
                # 平均分配
                tasks = self.try_divide_running_tasks(workers)
                new_tasks.extend(tasks)
                break
            break
        return new_tasks

    async def start(self):
        with tqdm(total=self._file_size, unit='B', unit_scale=True, unit_divisor=1024, desc=self._file_name,
                  leave=False, mininterval=10) as self._progress_bar:
            async with ClientSession(headers=self._headers) as self._session:
                # TODO 读取history
                # 初始化Worker的等待队列,第一个创建的Task是整个文件的
                task = WorkerTask(self._url, 0, self._file_size - 1, self._session, -1, self._headers, self._proxy)
                self._tasks.add_task(task)
                self.pending_task_queue.put_nowait(task)

                # 初始化worker
                self.spawn_workers(self._session)

                # 初始化Writer
                self.file_writer = TestFileWriter(self._file_name, self._file_size, self._progress_bar,
                                                  self.buffer_queue)

                concurrent_list = [worker.start_consuming() for worker in self.worker_instances]

                worker_group = asyncio.gather(*concurrent_list)

                # 启动workers,和writer
                done, pending = await asyncio.wait([worker_group, self.file_writer.start_consuming()],
                                                   return_when=asyncio.FIRST_COMPLETED)

                await asyncio.sleep(1)
                # 剩下的一定是writer
                await self.buffer_queue.put(None)
                await pending.pop()
                # 等待所有任务缓冲区中的数据都写入文件
                # await self.buffer_queue.join()
                print("下载完成")

    def spawn_workers(self, session):
        """
        创建worker实例,此时worker都被添加到了self.request_queue中了
        :param session:
        :return:
        """
        self._session = session
        for i in range(self._workers):
            worker_instance = DownloadWorker(self, i + 1)
            self.worker_instances.append(worker_instance)

    def reset_progress_bar_postfix(self):
        register_workers = self._workers
        working_workers = len(
            [worker for worker in self.worker_instances if worker.state == DownloadWorker.State.WORKING])
        print(f'活跃线程数:{working_workers} / {register_workers}')
        print(self.worker_instances)
        # TODO
        # self._progress_bar.set_postfix_str(f'活跃线程数:{working_workers} / {register_workers}')

    def worker_active_event_handler(self, e: WorkerActiveEvent):
        self.reset_progress_bar_postfix()

    def worker_inactive_event_handler(self, e: WorkerInactiveEvent):
        """
        manager接收到worker的inactive事件，判断worker的task状态，按需要再分配任务
        :param e: 事件
        :return:
        """
        # TODO 用来更新进度条的，细化worker_state和task_state组合的情况
        self.reset_progress_bar_postfix()

        if e.sender.task is not None:
            print(f"{e.sender.id}号Worker:任务完成:{e.sender.task}")
            # 更新task的所有权
            e.sender.task.update_owner(None)

            # 更新task的状态
            self._tasks.update_task_state(e.sender.task)

        self._workers_request_num += 1
        # 添加原worker_request_task_event_handler的逻辑
        if not self.task_dispatch():
            # 下载结束
            for _ in range(self._workers):
                self.pending_task_queue.put_nowait(None)
            print("下载结束")
        else:
            # 正常分配
            pass

    # TODO 将来考虑改为有最大长度的队列，当队列满了，就不再接收buffer，并挂起worker
    def worker_buffer_full_event_handler(self, e: WorkerBufferFullEvent):
        """
        manager接收到worker的buffer满的事件，将buffer加入到任务队列中
        :param e: 事件
        :return:manager是否成功的接受了事件
        """
        try:
            self.buffer_queue.put_nowait((e.current_byte, e.buffer))
            print(f"{e.sender.id}号Worker:buffer满了，已经加入到文件写入队列中,总长{len(e.buffer)}")
        except asyncio.QueueFull:
            e.reject()
        pass

    def worker_received_task_event_handler(self, e: WorkerReceivedTaskEvent):
        """
        manager接收到worker的task事件，self._workers_request_num+=1
        :param e: 事件
        """
        if self.pending_task_queue.qsize() == 0 and self._workers_request_num >= self._workers // 4:
            if not self.task_dispatch():
                # 下载结束
                for _ in range(self._workers):
                    self.pending_task_queue.put_nowait(None)
                print("下载结束")
            else:
                # 正常分配
                pass
        else:
            # 正常分配
            pass

    def task_dispatch(self):
        """
        任务分配
        :return:
        """
        if self._workers_request_num >= self._workers // 2:
            tasks = self.try_feed_waiting_workers_task(self._workers_request_num)
            print(
                f"{self._workers_request_num}个worker等待任务:{[worker.id for worker in self.worker_instances if worker.state == DownloadWorker.State.WAITING]}")
            if rv := (tasks != []):
                self._tasks.add_tasks(tasks)
                for task in tasks:
                    self.pending_task_queue.put_nowait(task)
                    print(f"分配任务:{task}")
                    self._workers_request_num -= 1
            print(f"分配了{len(tasks)}个任务")
            return rv
        else:
            # dismiss
            return False
