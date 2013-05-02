import logging
import Queue

queue = Queue.Queue()


class JobError(Exception):
    '''Error to be raised by jobs so that message is returned'''
    pass


class CapturingHandler(logging.Handler):
    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        self.queue.put(record)

handler = CapturingHandler(queue)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
