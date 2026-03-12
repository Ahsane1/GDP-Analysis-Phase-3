from abc import ABC, abstractmethod

class Observer(ABC):
    @abstractmethod
    def update(self, raw_size: int, processed_size: int):
        """
        CONTRACT: Any dashboard MUST have an update method 
        that accepts the sizes of the two queues.
        """
        pass

class Subject(ABC):
    def _init_(self):
        self._observers = []

    def add_observer(self, observer: Observer):
        self._observers.append(observer)

    def remove_observer(self, observer: Observer):
        self._observers.remove(observer)

    @abstractmethod
    def notify_observers(self):
        """
        CONTRACT: The telemetry monitor MUST have a way 
        to shout out to all its subscribers.
        """
        pass