from typing import Callable


class BaseEvent:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs

    def __repr__(self):
        return f"BaseEvent(name={self.name}, kwargs={self.kwargs})"

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if not isinstance(other, BaseEvent):
            return False
        return self.name == other.name and self.kwargs == other.kwargs


# 单播事件
class Event(BaseEvent):
    def __init__(self, sender, receiver: Callable, name, **kwargs):
        super().__init__(name, **kwargs)

        self.sender = sender
        self.receiver = receiver
        self.kwargs = kwargs
        self._accept = True

    def accept(self, **kwargs):
        self._accept = True

    def reject(self, **kwargs):
        self._accept = False

    def is_accept(self, **kwargs):
        return self._accept

    def trigger(self, **kwargs):
        self.receiver(self)

    def __repr__(self):
        return f"Event(sender={self.sender}, receiver={self.receiver}, name={self.name}, kwargs={self.kwargs})"


# 多播事件
class MultiEvent(Event):
    def __init__(self, sender, receivers: list, name, **kwargs):
        super().__init__(name, **kwargs)

        self.sender = sender
        self.receivers = receivers
        self.kwargs = kwargs
        self._accepted = {receiver: True for receiver in self.receivers}

    def trigger(self, **kwargs):
        for receiver in self.receivers:
            receiver(self, self.sender, **kwargs, **self.kwargs)

    def accept(self, **kwargs):
        """
        :param kwargs: receiver=receiver, ...
        :return:
        """
        receiver = kwargs.get("receiver")
        if receiver is None:
            raise ValueError("receiver must be specified")
        self._accepted[receiver] = True

    def reject(self, **kwargs):
        """
        :param kwargs: receiver=receiver, ...
        :return:
        """
        receiver = kwargs.get("receiver")
        if receiver is None:
            raise ValueError("receiver must be specified")
        self._accepted[receiver] = False

    def is_accept(self, **kwargs):
        """
        :param kwargs: receiver=receiver, ...
        :return:
        """
        receiver = kwargs.get("receiver")
        if receiver is None:
            raise ValueError("receiver must be specified")
        return self._accepted[receiver]

    def is_all_accept(self):
        return all(self._accepted.values())

    def __repr__(self):
        return f"MultiEvent(sender={self.sender}, receivers={self.receivers}, name={self.name}, kwargs={self.kwargs})"


class WorkerBufferFullEvent(Event):
    def __init__(self, sender, receiver: Callable, **kwargs):
        super().__init__(sender, receiver, self.__class__.__name__, **kwargs)
        self.buffer = None
        self.current_byte = None

    def trigger(self, **kwargs):
        self.buffer = kwargs.get("buffer")
        self.current_byte = kwargs.get("current_byte")
        super().trigger(**kwargs)


class WorkerActiveEvent(Event):
    def __init__(self, sender, receiver: Callable, **kwargs):
        super().__init__(sender, receiver, self.__class__.__name__, **kwargs)

    def trigger(self, **kwargs):
        super().trigger(**kwargs)


class WorkerInactiveEvent(Event):
    def __init__(self, sender, receiver: Callable, **kwargs):
        super().__init__(sender, receiver, self.__class__.__name__, **kwargs)

    def trigger(self, **kwargs):
        super().trigger(**kwargs)


class WorkerRequestTaskEvent(Event):
    def __init__(self, sender, receiver: Callable, **kwargs):
        super().__init__(sender, receiver, self.__class__.__name__, **kwargs)

    def trigger(self, **kwargs):
        super().trigger(**kwargs)


class WorkerReceivedTaskEvent(Event):
    def __init__(self, sender, receiver: Callable, **kwargs):
        super().__init__(sender, receiver, self.__class__.__name__, **kwargs)
        self.task = None

    def trigger(self, **kwargs):
        self.task = kwargs.get("task")
        super().trigger(**kwargs)
