import uuid
import datetime
import sys
import json
import traceback

import flask
#from flask.ext.admin import Admin
import werkzeug
import apscheduler.scheduler as apscheduler
import apscheduler.events as events
import sqlalchemy.sql as sql
import sqlalchemy as sa
import requests

import db
import util

import default_settings

#to be filled by sync async decorators
sync_types = {}
async_types = {}

app = flask.Flask(__name__)
scheduler = apscheduler.Scheduler()
#Allow a day for jobs to be run otherwise drop them. Should rerun these later.
scheduler.misfire_grace_time = 3600


def configure():
    app.config.from_object(default_settings)
    app.config.from_envvar('JOB_CONFIG', silent=True)
    db_url = app.config.get('SQLALCHEMY_DATABASE_URI')
    if not db_url:
        raise Exception('No db_url in config')
    db.setup_db(app)
    scheduler.add_listener(job_listener,
                           events.EVENT_JOB_EXECUTED |
                           events.EVENT_JOB_MISSED |
                           events.EVENT_JOB_ERROR)
    #Admin(app)


class RunNowTrigger(object):
    ''' custom apsceduler trigger to run job once and only
    once'''
    def __init__(self):
        self.run = False

    def get_next_fire_time(self, start_date):
        if not self.run:
            self.run = True
            return datetime.datetime.now()

    def __str__(self):
        return 'RunTriggerNow, run = %s' % self.run

    def __repr__(self):
        return 'RunTriggerNow, run = %s' % self.run


def job_listener(event):
    '''listens to completed job'''
    job_id = event.job.args[0]
    update_dict = {'finished_timestamp': datetime.datetime.now()}

    if event.code == events.EVENT_JOB_MISSED:
        update_dict['status'] = 'error'
        update_dict['error'] = json.dumps(
            'Job delayed too long, service full')
    elif event.exception:
        update_dict['status'] = 'error'
        if isinstance(event.exception, util.JobError):
            update_dict['error'] = json.dumps(event.exception.message)
        else:
            update_dict['error'] = \
                json.dumps(traceback.format_tb(event.traceback)[-1]
                           +
                           repr(event.exception))
    else:
        update_dict['status'] = 'complete'
        update_dict['data'] = json.dumps(event.retval)

    update_job(job_id, update_dict)
    result_ok = send_result(job_id)

    if not result_ok:
        ## TODO this clobbers original error
        update_dict['error'] = json.dumps(
            'Process completed but unable to post to result_url')
        update_job(job_id, update_dict)


headers = {'Content-Type': 'application/json'}


@app.route("/", methods=['GET'])
def index():
    return flask.jsonify(
        help="""Submit long running jobs.
        ``/status``: Status information
        ``/job``: List jobssubmit jobs with POST
        ``/job/<id>``: Get information about a specific job
        ``/job/<id>/data``: Get results from job
        """
    )


@app.route("/status", methods=['GET'])
def status():
    job_types = async_types.keys() + sync_types.keys()
    return flask.jsonify(
        version=0.1,
        job_types=job_types,
        name=app.config.get('NAME', 'example')
    )


@app.route("/job", methods=['GET'])
def job_list():
    args = dict((key, value) for key, value in flask.request.args.items())
    limit = args.pop('_limit', 100)
    offset = args.pop('_offset', 0)

    select = sql.select(
        [db.jobs_table.c.job_id],
        from_obj=[db.jobs_table.outerjoin(
            db.metadata_table,
            db.jobs_table.c.job_id == db.metadata_table.c.job_id)
        ]).\
        group_by(db.jobs_table.c.job_id).\
        order_by(db.jobs_table.c.requested_timestamp.desc()).\
        limit(limit).offset(offset)

    status = args.pop('_status', None)
    if status:
        select = select.where(db.jobs_table.c.status == status)

    ors = []
    for key, value in args.iteritems():
        ors.append(sql.and_(db.metadata_table.c.key == key,
                   db.metadata_table.c.value == value))

    if ors:
        select = select.where(sql.or_(*ors))
        select = select.having(
            sql.func.count(db.jobs_table.c.job_id) == len(ors)
        )

    result = db.engine.execute(select)
    listing = []
    for (job_id,) in result:
        listing.append(flask.url_for('job_status', job_id=job_id))

    return flask.jsonify(list=listing)


@app.route("/job/<job_id>", methods=['GET'])
def job_status(job_id):
    job_status = get_job_status(job_id)
    if not job_status:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    job_status.pop('api_key', None)
    return flask.jsonify(job_status)


@app.route("/job/<job_id>/data", methods=['GET'])
def job_data(job_id):
    job_status = get_job_status(job_id)
    if not job_status:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    if job_status['error']:
        return json.dumps({'error': job_status['error']}), 409, headers
    content_type = job_status['metadata'].get('mimetype')
    return flask.Response(job_status['data'], mimetype=content_type)


@app.route("/job/<job_id>/resubmit", methods=['POST'])
def resubmit_job(job_id):
    conn = db.engine.connect()
    job = conn.execute(db.jobs_table.select().where(
                       db.jobs_table.c.job_id == job_id)).first()
    if not job:
        return json.dumps({"error": ('Job does not exist')}), 409, headers
    if job['status'] != 'error':
        return json.dumps({"error": (
            'Cannot resubmit job with status {}'.format(
                job['status']))}), 409, headers
    input = {
        'data': json.loads(job['sent_data']),
        'job_type': job['job_type'],
        'api_key': job['api_key'],
        'metadata': get_metadata(job_id)
    }
    syncronous_job = sync_types.get(job['job_type'])
    if syncronous_job:
        return run_syncronous_job(syncronous_job, job_id, input, True)
    else:
        asyncronous_job = async_types.get(job['job_type'])
        return run_asyncronous_job(asyncronous_job, job_id, input, True)


@app.route("/job/<job_id>", methods=['POST'])
@app.route("/job", methods=['POST'])
def job(job_id=None):
    if not job_id:
        job_id = str(uuid.uuid4())

    ############# ERROR CHECKING ################
    try:
        input = flask.request.json
    except werkzeug.exceptions.BadRequest, e:
        return json.dumps({"error": "Malformed json"}), 409, headers

    if not flask.request.json:
        return json.dumps({"error": ('Not recognised as json, make '
                                     'sure content type is application/'
                                     'json')}), 409, headers

    ACCEPTED_ARGUMENTS = set(['job_type', 'data', 'metadata',
                              'result_url', 'api_key', 'metadata'])
    extra_keys = set(input.keys()) - ACCEPTED_ARGUMENTS
    if extra_keys:
        return json.dumps({"error": (
            'Too many arguments. Extra keys are {}'.format(
                ', '.join(extra_keys)))}), 409, headers

    #check result_url here as good to give warning early.
    result_url = input.get('result_url')
    if result_url and not result_url.startswith('http'):
        return json.dumps({"error": "result_url has to start with http"}), \
            409, headers

    job_type = input.get('job_type')
    if not job_type:
        return json.dumps({"error": "Please specify a job type"}), 409, headers

    job_types = async_types.keys() + sync_types.keys()

    if job_type not in job_types:
        error_string = (
            'Job type {} not available. Available job types are {}'
        ).format(job_type, ', '.join(job_types))
        return json.dumps({"error": error_string}), 409, headers

    metadata = input.get('metadata', {})
    if not isinstance(metadata, dict):
        return json.dumps({"error": "metadata has to be a json object"}), \
            409, headers
    ############# END CHECKING ################

    syncronous_job = sync_types.get(job_type)
    if syncronous_job:
        return run_syncronous_job(syncronous_job, job_id, input)
    else:
        asyncronous_job = async_types.get(job_type)
        return run_asyncronous_job(asyncronous_job, job_id, input)


def run_syncronous_job(job, job_id, input, resubmitted=False):
    # resubmitted jobs do not have to be stored
    try:
        if not resubmitted:
            store_job(job_id, input)
    except sa.exc.IntegrityError, e:
        error_string = 'job_id {} already exists'.format(job_id)
        return json.dumps({"error": error_string}), 409, headers

    update_dict = {}
    try:
        result = job(job_id, input)
        update_dict['status'] = 'complete'

        if hasattr(result, "__call__"):
            update_job(job_id, update_dict)
            return flask.Response(result(), mimetype='application/json')

        update_dict['data'] = json.dumps(result)
    except util.JobError, e:
        update_dict['status'] = 'error'
        update_dict['error'] = json.dumps(e.message)
    except Exception, e:
        update_dict['status'] = 'error'
        update_dict['error'] = json.dumps(traceback.format_tb(sys.exc_traceback)[-1]
                                          +
                                          repr(e))

    update_dict['finished_timestamp'] = datetime.datetime.now()

    update_job(job_id, update_dict)
    result_ok = send_result(job_id)

    if not result_ok:
        update_dict['error'] = json.dumps('Process completed but unable to '
                                          'post to result_url')
        update_job(job_id, update_dict)

    return job_status(job_id)


def run_asyncronous_job(job, job_id, input, resubmitted=False):
    # resubmitted jobs do not have to be stored
    if not scheduler.running:
        scheduler.start()

    try:
        if not resubmitted:
            store_job(job_id, input)
    except sa.exc.IntegrityError, e:
        error_string = 'job_id {} already exists'.format(job_id)
        return json.dumps({"error": error_string}), 409, headers

    scheduler.add_job(RunNowTrigger(), job, [job_id, input], None)

    return flask.jsonify(job_id=job_id)


def store_job(job_id, input):
    metadata = input.get('metadata', {})

    conn = db.engine.connect()
    trans = conn.begin()
    try:
        conn.execute(db.jobs_table.insert().values(
            job_id=job_id,
            job_type=input['job_type'],
            status='pending',
            requested_timestamp=datetime.datetime.now(),
            sent_data=json.dumps(input.get('data', {})),
            result_url=input.get('result_url'),
            api_key=input.get('api_key'))
        )
        inserts = []
        for key, value in metadata.items():
            type = 'string'
            if not isinstance(value, basestring):
                value = json.dumps(value)
                type = 'json'
            inserts.append(
                {"job_id": job_id,
                 "key": key,
                 "value": value,
                 "type": type}
            )
        if inserts:
            conn.execute(db.metadata_table.insert(), inserts)
        trans.commit()
    except Exception, e:
        trans.rollback()
        raise
    finally:
        conn.close()


def update_job(job_id, update_dict):
    db.engine.execute(db.jobs_table.update()
                      .where(db.jobs_table.c.job_id == job_id)
                      .values(**update_dict))


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

    try:
        result = requests.post(result_url,
                               data=json.dumps(job_status),
                               headers=headers)
    except requests.ConnectionError as conne:
        return False

    return result.status_code == requests.codes.ok


def get_job_status(job_id):
    result_dict = {}
    result = db.engine.execute(db.jobs_table.select()
                               .where(db.jobs_table.c.job_id == job_id)
                               ).first()
    if not result:
        return None
    fields = result.keys()
    for field in fields:
        value = getattr(result, field)
        if value is None:
            result_dict[field] = value
        elif field in ('sent_data', 'data', 'error'):
            result_dict[field] = json.loads(value)
        elif isinstance(value, datetime.datetime):
            result_dict[field] = value.isoformat()
        else:
            result_dict[field] = unicode(value)
    result_dict['metadata'] = get_metadata(job_id)
    return result_dict


def get_metadata(job_id):
    results = db.engine.execute(db.metadata_table.select()
                                .where(db.metadata_table.c.job_id ==
                                       job_id)).fetchall()
    metadata = {}
    for row in results:
        value = row['value']
        if row['type'] == 'json':
            value = json.loads(value)
        metadata[row['key']] = value
    return metadata


def run():
    return app.run(port=int(app.config.get('PORT', 5000)))


def test_client():
    return app.test_client()
