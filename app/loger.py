import logging
import queue
from logging.handlers import RotatingFileHandler, QueueListener, QueueHandler

log_queue = queue.Queue()
queue_handler = QueueHandler(log_queue)

root = logging.getLogger()
root.addHandler(queue_handler)

rot_handler = RotatingFileHandler(...)
queue_listener = QueueListener(log_queue, rot_handler)
queue_listener.start()
