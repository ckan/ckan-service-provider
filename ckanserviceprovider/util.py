# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import datetime

import db


class JobError(Exception):
    '''Error to be raised by jobs so that message is returned'''
    pass


class StoringHandler(logging.Handler):
    '''A handler that stores the logging records
    in the database.'''
    def __init__(self, task_id, input):
        logging.Handler.__init__(self)
        self.task_id = task_id
        self.input = input

    def emit(self, record):
        conn = db.engine.connect()
        try:
            conn.execute(db.logs_table.insert().values(
                job_id=self.task_id,
                timestamp=datetime.datetime.now(),
                message=record.getMessage(),
                level=record.levelname,
                module=record.module,
                funcName=record.funcName,
                lineno=record.lineno))
        finally:
            conn.close()
