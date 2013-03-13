import logging

import ckanserviceprovider.job as job
import ckanserviceprovider.util as util

logging.basicConfig()


@job.sync
def echo(task_id, input):
    logging.warning('foo')
    if input['data'].startswith('>'):
        raise util.JobError('do not start message with >')
    if input['data'].startswith('#'):
        raise Exception('serious exception')
    return '>' + input['data']


@job.async
def async_echo(task_id, input):
    if input['data'].startswith('>'):
        raise util.JobError('do not start message with >')
    if input['data'].startswith('#'):
        raise Exception('serious exception')
    return '>' + input['data']
