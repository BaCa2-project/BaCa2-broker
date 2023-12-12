from threading import Thread, Lock
from datetime import timedelta, datetime
from time import sleep
from typing import Callable

from settings import APP_SETTINGS

DEFAULT_TIMEOUT = APP_SETTINGS['default_timeout']
DEFAULT_TIMESTEP = APP_SETTINGS['default_timestep']


class TimeoutTick(Thread):
    def __init__(self, master, timestep: timedelta):
        super().__init__()
        self.master = master
        self.timestep = timestep
        self._stop = False

    def run(self):
        while not self._stop:
            sleep(self.timestep.total_seconds())
            self.master.check_timeouts()
        print('TimeoutTick stopped')

    def stop(self):
        self._stop = True


class Timeout:
    def __init__(self,
                 timeout_id: str,
                 timeout: timedelta,
                 timeout_lock: Lock,
                 success_ping: Callable[[bool], None]) -> None:
        self.timeout_id = timeout_id
        self.start_time = datetime.now()
        self.timeout = timeout
        self.timeout_lock = timeout_lock
        self.success_ping = success_ping

        self.timeout_lock.acquire()

    def check(self, resolve_if_expired: bool = True) -> bool:
        if datetime.now() - self.start_time > self.timeout:
            if resolve_if_expired:
                self.resolve()
            return True
        return False

    def resolve(self) -> None:
        self.success_ping(False)
        self.timeout_lock.release()


class ActiveWait(Timeout):
    def __init__(self,
                 timeout_id: str,
                 timeout: timedelta,
                 timeout_lock: Lock,
                 success_ping: Callable[[bool], None],
                 wait_action: Callable[[], bool]) -> None:
        super().__init__(timeout_id, timeout, timeout_lock, success_ping)
        self.wait_action = wait_action

    def check(self, resolve_if_expired: bool = True) -> bool:
        if self.wait_action() and resolve_if_expired:
            self.resolve(success=True)
            return True
        if datetime.now() - self.start_time > self.timeout:
            if resolve_if_expired:
                self.resolve()
            return True
        return False

    def resolve(self, success: bool = False) -> None:
        self.success_ping(success)
        self.timeout_lock.release()


class TimeoutManager:

    def __init__(self,
                 timeout: timedelta = DEFAULT_TIMEOUT,
                 timestep: timedelta = DEFAULT_TIMESTEP) -> None:
        self.timeout = timeout
        self.timestep = timestep
        self.tick = TimeoutTick(self, self.timestep)
        self.tick.start()
        self.timeouts = {}
        self.timeout_modification_lock = Lock()

    def __del__(self):
        self.stop()

    def stop(self):
        self.tick.stop()
        try:
            self.tick.join()
        except TypeError:
            pass

    def check_timeouts(self) -> None:
        with self.timeout_modification_lock:
            timeouts_to_remove = []
            for timeout in self.timeouts.values():
                if timeout.check():
                    timeouts_to_remove.append(timeout.timeout_id)
        for timeout_id in timeouts_to_remove:
            self.remove_timeout(timeout_id)

    def add_timeout(self,
                    timeout_id: str,
                    timeout_lock: Lock,
                    success_ping: Callable[[bool], None],
                    timeout: timedelta = DEFAULT_TIMEOUT, ) -> None:
        with self.timeout_modification_lock:
            self.timeouts[timeout_id] = Timeout(timeout_id, timeout, timeout_lock, success_ping)

    def add_active_wait(self,
                        timeout_id: str,
                        timeout_lock: Lock,
                        success_ping: Callable[[bool], None],
                        wait_action: Callable[[], bool],
                        timeout: timedelta = DEFAULT_TIMEOUT, ) -> None:
        with self.timeout_modification_lock:
            self.timeouts[timeout_id] = ActiveWait(timeout_id, timeout, timeout_lock, success_ping, wait_action)

    def remove_timeout(self, timeout_id: str) -> None:
        with self.timeout_modification_lock:
            if timeout_id in self.timeouts.keys():
                del self.timeouts[timeout_id]
