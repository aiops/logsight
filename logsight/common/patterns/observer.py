from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Optional


class Subject(ABC):
    """
        This is an implementation of the  observer pattern:
        https://refactoring.guru/design-patterns/observer/python/example
    """

    def __init__(self):
        self._state: Optional[Any] = None
        self._observers: List[SubjectObserver] = []

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value: Subject):
        self._state = value
        self.notify()

    def attach(self, observer: SubjectObserver) -> None:
        self._observers.append(observer)

    def detach(self, observer: SubjectObserver) -> None:
        self._observers.remove(observer)

    def notify(self) -> None:
        for observer in self._observers:
            observer.on_update(self._state)


class SubjectObserver(ABC):
    """
        This is an implementation of the  observer pattern:
        https://refactoring.guru/design-patterns/observer/python/example
    """

    @abstractmethod
    def on_update(self, state: Subject) -> None:
        raise NotImplementedError
