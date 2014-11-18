# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
import datetime

import db


class JobError(Exception):
    '''Error to be raised by jobs so that message is returned'''
    pass


class HttpError(JobError):
    '''A Job Error raised by jobs that have are caused by http errors'''
    def __init__(self, message, http_code=None, request_url=None,
                 response=None):
        super(JobError, self).__init__(message)
        self.http_code=http_code
        self.request_url=request_url
        self.response=response


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
