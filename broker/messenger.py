from abc import ABC, abstractmethod


class KolejkaMessengerInterface(ABC):
    def __init__(self, master):
        self.master = master

    @abstractmethod
    def send(self, x: ...):
        ...


class BacaMessengerInterface(ABC):
    def __init__(self, master):
        self.master = master

    @abstractmethod
    def send(self, x: ...):
        ...

    @abstractmethod
    def send_error(self, x: ...):
        ...
