import uuid
import datetime
import os
import ConfigParser
import json
import traceback

import flask
import werkzeug
import apscheduler.scheduler as apscheduler
import apscheduler.events as events
import requests
import sqlalchemy as sa

import synchronous_jobs
import asynchronous_jobs
import db
import util

job_types = ['example']
app = flask.Flask(__name__)
scheduler = apscheduler.Scheduler()
#Allow a day for jobs to be run otherwise drop them. Should rerun these later.
scheduler.misfire_grace_time = 3600

def configure():
    config = ConfigParser.ConfigParser()
    config_file = os.environ.get('JOB_CONFIG')
    if config_file:
        config.read(config_file)
        for key, value in config.items('app:job'):
            app.config[key] = value
    db_url = app.config.get('DB_URL')
    if not db_url:
        db_url = config.get('app:main', 'sqlalchemy.url')
    if not db_url:
        raise Exception('No db_url in config')
    db.setup_db(db_url)
    scheduler.add_listener(job_listener,
                           events.EVENT_JOB_EXECUTED | 
                           events.EVENT_JOB_MISSED | 
                           events.EVENT_JOB_ERROR)


def job_listener(event):
    '''listens to completed job'''
    job_id = event.job.args[0]
    update_dict = {'finished_timestamp': datetime.datetime.now()}

    if event.exception:
        update_dict['status'] = 'error'
        if isinstance(event.exception, util.JobError):
            update_dict['error'] = json.dumps(event.exception.message)
        else:
            update_dict['error'] =\
                    json.dumps('\n'.join(traceback.format_tb(event.traceback)) 
                               + repr(event.exception))
    else:
        update_dict['status'] = 'complete'
        update_dict['data'] = json.dumps(event.retval)

    update_job(job_id, update_dict)
    result_ok = send_result(job_id)

    if not result_ok:
        update_dict['error'] = json.dumps('Process completed but unable to post to result_url')
        update_job(job_id, update_dict)


headers = {'Content-Type': 'aplication/json'}

@app.route("/status", methods=['GET'])
def stutus():
    return flask.jsonify(
        version=0.1,
        job_types=job_types,
        name='datastorer'
    )

@app.route("/job", methods=['GET'])
def job_list():
    return

@app.route("/job/<job_id>", methods=['GET'])
def job_status(job_id):
    job_status = get_job_status(job_id)
    if not job_status:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    job_status.pop('api_key', None)
    return json.dumps(job_status), 200, headers

@app.route("/job/<job_id>", methods=['POST'])
@app.route("/job", methods=['POST'])
def job(job_id=None):
    if not job_id:
        job_id = str(uuid.uuid4())

    try:
        input = flask.request.json
    except werkzeug.exceptions.BadRequest, e:
        return json.dumps({"error": "Malformed json"}), 409, headers

    if not flask.request.json:
        return json.dumps(
            {"error": ('Not recognised as json, make '
                       'sure content type is application/json')
            }), 409, headers

    job_type = input.get('job_type')
    if not job_type:
        return json.dumps({"error": "Please specify a job type"}), 409, headers

    if job_type not in job_types:
        error_string = (
            'Job type {} not availiable. Availible job types are {}'
        ).format(job_type, ', '.join(job_types))
        return json.dumps({"error": error_string}), 409, headers

    syncronous_job = getattr(synchronous_jobs, job_type, None)
    if syncronous_job:
        return run_syncronous_job(syncronous_job, job_id, input)
    else:
        asyncronous_job = getattr(asynchronous_jobs, job_type, None)
        if not asyncronous_job:
            raise Exception('No job of type {}'.format(job_type))
        return run_asyncronous_job(asyncronous_job, job_id, input)


def run_syncronous_job(job, job_id, input):
    pass

def run_asyncronous_job(job, job_id, input):
    if not scheduler.running:
        scheduler.start()

    exec_date = datetime.datetime.now() + datetime.timedelta(seconds = 0.1)
    try:
        store_job(job_id, input)
    except sa.exc.IntegrityError, e:
        error_string = 'job_id {} already exists'.format(job_id)
        return json.dumps({"error": error_string}), 409, headers
        
    scheduler.add_date_job(job, exec_date, [job_id, input])
    return flask.jsonify(job_id=job_id)


def store_job(job_id, input):
    metadata = json.dumps(input['metadata']) if input.get('metadata') else None
    db.engine.execute(db.task_table.insert().values(
        job_id=job_id,
        job_type=input['job_type'],
        status='pending',
        requested_timestamp=datetime.datetime.now(),
        sent_data=json.dumps(input['data']),
        metadata=metadata,
        result_url=input.get('result_url'),
        api_key=input.get('api_key')
        )
    )

def update_job(job_id, update_dict):
    db.engine.execute(db.task_table.update()
                      .where(db.task_table.c.job_id == job_id)
                      .values(**update_dict)
                     )

def send_result(job_id):
    ''' Send results to where requested. '''
    job_status = get_job_status(job_id)
    result_url = job_status.get('result_url')
    if not result_url:
        return True
    api_key = job_status.pop('api_key', None)
    headers = {'Content-Type': 'application/json'}
    if api_key:
        if ':' in api_key:
            header, key = api_key.split(':')
        else:
            header, key = 'Authorization', api_key
        headers[header] = key

    result = requests.post(result_url, 
                           data=json.dumps(job_status), 
                           headers=headers)

    return result.status_code == requests.codes.ok
    
def get_job_status(job_id):
    result_dict = {}
    result = db.engine.execute(db.task_table.select()
                               .where(db.task_table.c.job_id == job_id)
                               ).first()
    if not result:
        return None
    fields = result.keys()
    for field in fields:
        value = getattr(result, field)
        if value is None:
            result_dict[field] = value
        elif field in ('sent_data', 'data', 'metadata', 'error'):
            result_dict[field] = json.loads(value)
        elif isinstance(value, datetime.datetime):
            result_dict[field] = value.isoformat()
        else:
            result_dict[field] = unicode(value)
    return result_dict
         
if __name__ == "__main__":
    configure()
    app.run()
