import enum
import functools
from math import floor
from typing import Optional, List, Iterable


class WorkerTask:
    # 256KB
    SAFE_CHUNK_SIZE = 256 * 1024

    # 因为是协程,所以是线程安全的
    NEXT_ID = 0

    def __init__(self, url, start_byte, end_byte, session, parent_id, headers=None, proxy=None):
        self.url = url
        self.start_byte = start_byte
        self.current_byte = start_byte
        self.end_byte = end_byte
        self.session = session
        self.headers = headers
        self.proxy = proxy
        self._state = WorkerTask.State.PENDING
        self.worker_id = None

        # 用于重启下载后的任务回溯,谁的parent_id == -1 表示他是根任务
        self.id = WorkerTask.NEXT_ID
        WorkerTask.NEXT_ID += 1
        self.parent_id = parent_id

    def update_state(self, state):
        self._state = state

    def get_state(self):
        return self._state

    def is_finished(self):
        return self._state == WorkerTask.State.FINISHED

    def is_error(self):
        return self._state == WorkerTask.State.ERROR

    def is_running(self):
        return self._state == WorkerTask.State.RUNNING

    def get_size(self):
        return self.end_byte - self.start_byte + 1

    def update_owner(self, worker_id):
        self.worker_id = worker_id

    def try_divide(self) -> Optional['WorkerTask']:
        """
        将任务分割为两个任务
        :return:
        """
        if self.dividable():
            new_task_begin = (self.end_byte - self.current_byte) // 2 + self.current_byte
            new_task = WorkerTask(self.url, new_task_begin, self.end_byte, self.session, self.id, self.headers,
                                  self.proxy)
            self.end_byte = new_task_begin - 1
            return new_task
        else:
            return None

    def try_divides(self, task_num) -> List['WorkerTask']:
        """
        将任务分割为task_num个,或者最接近于task_num个（如果dividable_time<task_num）任务
        :param task_num:
        :return:
        """
        new_tasks = []
        if self.dividable():
            new_task_end = 0
            new_task_begin = 0

            dividable_time = min(self.dividable_times(), task_num)
            new_tasks_size = floor((self.end_byte - self.current_byte) / dividable_time)
            for i in range(dividable_time):
                new_task_end = self.end_byte - i * new_tasks_size
                new_task_begin = new_task_end - new_tasks_size + 1
                new_tasks.append(
                    WorkerTask(self.url, new_task_begin, new_task_end, self.session, self.id, self.headers,
                               self.proxy))
            self.end_byte = new_task_begin - 1
        return new_tasks

    def dividable(self) -> bool:
        if self._state in [WorkerTask.State.RUNNING, WorkerTask.State.PENDING]:
            if self.current_byte + 2 * self.SAFE_CHUNK_SIZE < self.end_byte:
                return True
        return False

    def dividable_times(self) -> int:
        if self.dividable():
            if self.is_running():
                return floor((self.end_byte - self.current_byte) / self.SAFE_CHUNK_SIZE) - 1
            elif self._state == WorkerTask.State.PENDING:
                return floor((self.end_byte - self.start_byte) / self.SAFE_CHUNK_SIZE)
        else:
            return 0

    def __eq__(self, other):
        return self.url == other.url and self.start_byte == other.start_byte and self.end_byte == other.end_byte

    def __hash__(self):
        return hash((self.url, self.start_byte, self.end_byte))

    class State(enum.Enum):
        # TASK_NOT_FOUND = -1
        PENDING = 0
        RUNNING = 1
        FINISHED = 2
        ERROR = 3
        # TODO 还没有写PAUSED的处理逻辑
        PAUSED = 4


class WorkerTaskMaxHeap:
    def __init__(self, tasks=None):
        if tasks is None:
            tasks = []
        self.heap = []
        self.task_indices = {}
        self._build_heap(tasks)

    def _build_heap(self, tasks):
        for task in tasks:
            self.heap.append(task)
            self.task_indices[task] = len(self.heap) - 1
        self._heapify()

    def _heapify(self):
        for i in range(len(self.heap) // 2 - 1, -1, -1):
            self._sift_down(i)

    def _sift_down(self, index):
        task = self.heap[index]
        while index * 2 + 1 < len(self.heap):
            child_index = index * 2 + 1
            if child_index + 1 < len(self.heap) and self.heap[child_index + 1].get_size() > self.heap[
                child_index].get_size():
                child_index += 1
            if self.heap[child_index].get_size() > self.heap[index].get_size():
                self.heap[child_index], self.heap[index] = self.heap[index], self.heap[child_index]
                self.task_indices[self.heap[child_index]] = child_index
                self.task_indices[self.heap[index]] = index
                index = child_index
            else:
                break

    def get_max(self) -> WorkerTask:
        return self.heap[0]

    def pop_max(self) -> WorkerTask:
        max_task = self.heap[0]
        last_task = self.heap.pop()
        if self.heap:
            self.heap[0] = last_task
            self.task_indices[last_task] = 0
            self._sift_down(0)
        del self.task_indices[max_task]
        return max_task

    def push(self, task: WorkerTask):
        if task in self.task_indices:
            raise ValueError("Task already exists in the heap")
        self.heap.append(task)
        self.task_indices[task] = len(self.heap) - 1
        self._sift_up(len(self.heap) - 1)

    def push_all(self, tasks: Iterable[WorkerTask]):
        for task in tasks:
            self.push(task)

    def _sift_up(self, index):
        task = self.heap[index]
        while index > 0:
            parent_index = (index - 1) // 2
            parent_task = self.heap[parent_index]
            if task.get_size() <= parent_task.get_size():
                break
            self.heap[index] = parent_task
            self.task_indices[parent_task] = index
            index = parent_index
        self.heap[index] = task
        self.task_indices[task] = index

    def get_tasks(self) -> List[WorkerTask]:
        return self.heap

    def remove(self, task: WorkerTask):
        index = self.task_indices[task]
        last_task = self.heap.pop()
        if index < len(self.heap):
            self.heap[index] = last_task
            self.task_indices[last_task] = index
            self._sift_down(index)
        del self.task_indices[task]

    def size(self):
        return len(self.heap)

    def __len__(self):
        return len(self.heap)
