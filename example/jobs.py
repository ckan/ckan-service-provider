import ckanserviceprovider.job as job
import ckanserviceprovider.util as util


@job.sync
def echo(task_id, input):
    if input['data'].startswith('>'):
        raise util.JobError('do not start message with >')
    return '>' + input['data']
