from abc import ABC, abstractmethod

class Observer(ABC):
    @abstractmethod
    def update(self, state: dict):
        pass

class Subject(ABC):
    def __init__(self):
        self._observers = []

    def attach(self, observer: Observer):
        if observer not in self._observers:
            self._observers.append(observer)

    @abstractmethod
    def notify_observers(self, state: dict):
        pass