import logging

import ckanserviceprovider.job as job
import ckanserviceprovider.util as util


@job.sync
def echo(task_id, input, queue):
    if input['data'].startswith('>'):
        raise util.JobError('do not start message with >')
    if input['data'].startswith('#'):
        raise Exception('serious exception')
    return '>' + input['data']


@job.async
def async_echo(task_id, input, queue):
    if input['data'].startswith('>'):
        raise util.JobError('do not start message with >')
    if input['data'].startswith('#'):
        raise Exception('serious exception')
    return '>' + input['data']


@job.async
def async_ping(task_id, input, queue):
    handler = util.QueuingHandler(queue)
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)

    logger.warn('ping')
    return "ping"
