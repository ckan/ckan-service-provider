import time
import util 

def example(task_id, input):
    if 'time' not in input['data']:
        raise util.JobError('time not in input')

    time.sleep(input['data']['time'])
    return 'Slept for '+ str(input['data']['time']) + ' seconds.'


