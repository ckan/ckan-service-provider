import logging

import ckanserviceprovider.job as job
import ckanserviceprovider.util as util


@job.synchronous
def echo(task_id, input):
    if input["data"].startswith(">"):
        raise util.JobError("do not start message with >")
    if input["data"].startswith("#"):
        raise Exception("serious exception")
    return ">" + input["data"]


@job.asynchronous
def async_echo(task_id, input):
    if input["data"].startswith(">"):
        raise util.JobError("do not start message with >")
    if input["data"].startswith("#"):
        raise Exception("serious exception")
    return ">" + input["data"]


@job.asynchronous
def async_ping(task_id, input):
    handler = util.StoringHandler(task_id, input)
    logger = logging.getLogger(__name__)
    logger.addHandler(handler)

    logger.warn("ping")
    return "ping"
