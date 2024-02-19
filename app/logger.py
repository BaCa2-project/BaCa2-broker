import logging
import queue
from logging.handlers import RotatingFileHandler, QueueListener, QueueHandler
from os import PathLike


class LoggerManager:

    def __init__(self, logger_name: str, file: str | PathLike[str], level: int):
        self.log_queue = queue.Queue()
        self.queue_handler = QueueHandler(self.log_queue)

        self.root = logging.Logger(logger_name, level)
        self.root.addHandler(self.queue_handler)

        self.rot_handler = RotatingFileHandler(file, maxBytes=1000000, backupCount=5, encoding='utf-8')
        self.queue_listener = QueueListener(self.log_queue, self.rot_handler)

    @property
    def logger(self):
        return self.root

    def set_formatter(self, format_: str):
        self.queue_handler.setFormatter(logging.Formatter(format_))

    def start(self):
        self.queue_listener.start()

    def stop(self):
        self.queue_listener.stop()
        self.rot_handler.close()
        self.queue_handler.close()
