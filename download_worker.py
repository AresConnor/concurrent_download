import enum
import asyncio
from typing import Optional

from events import WorkerBufferFullEvent, WorkerActiveEvent, WorkerInactiveEvent, WorkerRequestTaskEvent, Event, \
    WorkerReceivedTaskEvent
from task import WorkerTask


class DownloadWorker:
    worker_buffer_size = 1024 * 1024

    class State(enum.Enum):
        PENDING = 0
        WAITING = 1
        WORKING = 2
        PAUSED = 4
        EXIT = 5
        STOPPED = 3

    def __init__(self, manager, i):
        self._task: Optional[WorkerTask] = None
        self._manager = manager
        self._worker_id = i

        self._worker_state: DownloadWorker.State = DownloadWorker.State.WAITING
        self._worker_buffer: bytearray = bytearray()

        # events
        self.workerBufferFullEvent = WorkerBufferFullEvent(sender=self,
                                                           receiver=self._manager.worker_buffer_full_event_handler)
        self.workerInactiveEvent = WorkerInactiveEvent(sender=self,
                                                       receiver=self._manager.worker_inactive_event_handler)
        self.workerActiveEvent = WorkerActiveEvent(sender=self, receiver=self._manager.worker_active_event_handler)
        self.workerReceivedTaskEvent = WorkerReceivedTaskEvent(sender=self,
                                                               receiver=self._manager.worker_received_task_event_handler)

        self.on_worker_inactive(WorkerTask.State.FINISHED)

    async def run_task(self):

        self.on_worker_pending()

        # Worker working download Task ...
        # 更新请求头
        self._task.headers.update({
            "Range": f"bytes={self._task.start_byte}-{self._task.end_byte}"
        })
        try:
            async with self._task.session.get(self._task.url, headers=self._task.headers, proxy=self._task.proxy,
                                              timeout=60) as response:
                if not response.ok:
                    print(f"status error: {response.status}")
                    # send event
                    self.on_worker_inactive(WorkerTask.State.ERROR)
                    return

                # send worker active event
                self.on_worker_active()
                # 从响应流中读取指定长度
                async for data in response.content.iter_any():
                    # if received pause signal
                    if self._worker_state == DownloadWorker.State.PAUSED:
                        self.on_worker_inactive(WorkerTask.State.PAUSED)
                        while self._worker_state == DownloadWorker.State.PAUSED:
                            await asyncio.sleep(0.1)
                        self.on_worker_active()

                    if len(self._worker_buffer) > self.worker_buffer_size:
                        # send buffer full event
                        self.on_buffer_full()

                    if self._worker_state == DownloadWorker.State.STOPPED:
                        self.on_buffer_full()
                        self.on_worker_inactive(WorkerTask.State.ERROR)
                        break

                    data_len = len(data)
                    # receiving data from server
                    if self._task.current_byte + data_len <= self._task.end_byte:
                        self._worker_buffer.extend(data)

                    # receiving data from server end
                    else:
                        try:
                            self._worker_buffer.extend(data[data_len])
                        except Exception:
                            print(len(data), data_len)

                        # send buffer full event
                        self.on_buffer_full()
                        break

                # send event
                self.on_worker_inactive(WorkerTask.State.FINISHED)
                return

        except Exception as e:
            print(f"第{self._worker_id}号线程", e)
            # send event
            self.on_worker_inactive(WorkerTask.State.ERROR)

            return

    async def feed_task(self, task: WorkerTask):
        self._task = task
        # 更新所有权
        self._task.update_owner(self._worker_id)
        return await self.run_task()

    async def start_consuming(self):
        while True:
            if self._worker_state == DownloadWorker.State.STOPPED:
                break
            elif self._worker_state == DownloadWorker.State.WAITING:
                task = await self._manager.pending_task_queue.get()
                self.on_worker_task_received()
                # 接收到None则表示总下载已经结束
                if task is not None:
                    await self.feed_task(task)
                else:
                    # 代表下载结束
                    print(f"第{self._worker_id}号Worker退出")
                    break
            else:
                await asyncio.sleep(0.1)

    def on_buffer_full(self):
        if self._worker_buffer:
            self.workerBufferFullEvent.trigger(current_byte=self._task.current_byte, buffer=bytes(self._worker_buffer))
            if self.workerBufferFullEvent.is_accept():
                self._task.current_byte = self._task.current_byte + len(self._worker_buffer)
                self._worker_buffer.clear()
            else:
                self.pause()

    def on_worker_pending(self):
        self._worker_state = DownloadWorker.State.PENDING

    def on_worker_active(self):
        self._task.update_state(WorkerTask.State.RUNNING)
        self._task.update_owner(self._worker_id)
        self._worker_state = DownloadWorker.State.WORKING
        self.workerActiveEvent.trigger()

    def on_worker_inactive(self, task_state):
        if self._task is not None:
            self._task.update_state(task_state)
        self._worker_state = DownloadWorker.State.WAITING
        self.workerInactiveEvent.trigger()

    def on_worker_task_received(self):
        self.workerReceivedTaskEvent.trigger()

    def stop(self):
        self._worker_state = DownloadWorker.State.STOPPED

    def pause(self):
        """
        挂起worker
        :return:
        """
        if self._worker_state == DownloadWorker.State.WORKING:
            print(f"第{self._worker_id}号Worker挂起")
            self._worker_state = DownloadWorker.State.PAUSED

    def resume(self):
        """
        恢复worker
        :return:
        """
        if self._worker_state == DownloadWorker.State.PAUSED:
            print(f"第{self._worker_id}号Worker恢复")
            self._worker_state = DownloadWorker.State.WORKING

    @property
    def state(self):
        return self._worker_state

    @property
    def id(self):
        return self._worker_id

    @property
    def task(self):
        return self._task

    def __str__(self):
        return f"Worker {self._worker_id}, state={self._worker_state}, task_id={self.task.id if self._task else None}"

    def __repr__(self):
        return self.__str__()