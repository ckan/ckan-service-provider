# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import uuid
import datetime
import sys
import json
import traceback
import logging
import logging.handlers

import flask
import flask.ext.login as flogin
#from flask.ext.admin import Admin
import werkzeug
import apscheduler.scheduler as apscheduler
import apscheduler.events as events
import apscheduler.jobstores.sqlalchemy_store as sqlalchemy_store
import apscheduler.jobstores.shelve_store as shelve_store
import sqlalchemy.sql as sql
import sqlalchemy as sa
import requests
from werkzeug.contrib.fixers import ProxyFix

import db
import util
import default_settings

#to be filled by sync async decorators
sync_types = {}
async_types = {}
job_statuses = ['pending', 'complete', 'error']

app = flask.Flask(__name__)
scheduler = apscheduler.Scheduler()
#Allow a day for jobs to be run otherwise drop them. Should rerun these later.
scheduler.misfire_grace_time = 3600

app.url_map.strict_slashes = False


class User(flogin.UserMixin):
    def __init__(self, name, id, active=True):
        self.name = name
        self.id = id
        self.active = active

    def is_active(self):
        return self.active


class Anonymous(flogin.AnonymousUserMixin):
    name = u"Anonymous"

_users = None
_names = None


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

    # need this for the tests
    try:
        scheduler.remove_jobstore('default')
    except KeyError:
        pass
    scheduler.add_jobstore(sqlalchemy_store.SQLAlchemyJobStore(
        url=db_url), 'default')

    login_manager = flogin.LoginManager()
    login_manager.setup_app(app)

    login_manager.anonymous_user = Anonymous
    login_manager.login_view = "login"

    global _users
    global _names

    _users = {
        app.config['USERNAME']: User('Admin', 0)
    }

    _names = dict((int(v.get_id()), k) for k, v in _users.items())

    @login_manager.user_loader
    def load_user(userid):
        userid = int(userid)
        name = _names.get(userid)
        return _users.get(name)

    logger = logging.getLogger()

    if not app.debug:
        stderr_handler = logging.StreamHandler(
            sys.stderr)

        stderr_handler.setLevel(logging.INFO)

        file_handler = logging.handlers.RotatingFileHandler(
            app.config.get('LOG_FILE'),
            maxBytes=67108864, backupCount=5)
        file_handler.setLevel(logging.INFO)

        mail_handler = logging.handlers.SMTPHandler(
            '127.0.0.1',
            app.config.get('FROM_EMAIL'),
            app.config.get('ADMINS', []),
            'CKAN Service Error')
        mail_handler.setLevel(logging.ERROR)


        if 'LOG_FILE' in app.config:
            logger.addHandler(file_handler)
        if 'FROM_EMAIL' in app.config:
            logger.addHandler(mail_handler)
        if 'STDERR' in app.config:
            logger.addHandler(stderr_handler)
    elif not app.testing:
        logger.addHandler(app.logger.handlers[0])

    app.wsgi_app = ProxyFix(app.wsgi_app)


class RunNowTrigger(object):
    '''Custom apscheduler trigger to run job once and only
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
    '''Listens to completed job'''
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
                json.dumps(traceback.format_tb(event.traceback)
                           +
                           [repr(event.exception)])
    else:
        update_dict['status'] = 'complete'
        update_dict['data'] = json.dumps(event.retval)

    update_dict['api_key'] = None

    api_key = get_job(job_id)['api_key']
    update_job(job_id, update_dict)
    result_ok = send_result(job_id, api_key)

    if not result_ok:
        ## TODO this clobbers original error
        update_dict['error'] = json.dumps(
            'Process completed but unable to post to result_url')
        update_job(job_id, update_dict)


headers = {str('Content-Type'): str('application/json')}

@app.route("/", methods=['GET'])
def index():
    '''Show link to documentation.

    :rtype: A dictionary with the following keys
    :param help: Help text
    :type help: string
    '''
    return flask.jsonify(
        help="""
        Get help at:
        http://ckan-service-provider.readthedocs.org/."""
    )


@app.route("/status", methods=['GET'])
def status():
    '''Show version, available job types and name of service.

    **Results:**

    :rtype: A dictionary with the following keys
    :param version: Version of the service provider
    :type version: float
    :param job_types: Available job types
    :type job_types: list of strings
    :param name: Name of the service
    :type name: string
    :param stats: Shows stats for jobs in queue
    :type stats: dictionary
    '''
    job_types = async_types.keys() + sync_types.keys()

    counts = {}
    for job_status in job_statuses:
        counts[job_status] = db.engine.execute(
            db.jobs_table.count()
            .where(db.jobs_table.c.status == job_status)
        ).first()[0]

    return flask.jsonify(
        version=0.1,
        job_types=job_types,
        name=app.config.get('NAME', 'example'),
        stats=counts
    )


def check_auth(username, password):
    '''This function is called to check if a username /
    password combination is valid.
    '''
    return (username == app.config['USERNAME'] and
            password == app.config['PASSWORD'])


@app.route('/login', methods=['POST', 'GET'])
def login():
    '''Log in as administrator

    You can use wither basic auth or form based login (via POST).

    :param username: The administrator's username
    :type username: string
    :param password: The administrator's password
    :type password: string
    '''
    username = None
    password = None
    next = flask.request.args.get('next')
    auth = flask.request.authorization

    if flask.request.method == 'POST':
        username = flask.request.form['username']
        password = flask.request.form['password']

    if auth and auth.type == 'basic':
        username = auth.username
        password = auth.password

    if not flogin.current_user.is_active():
        error = 'You have to login with proper credentials'
        if username and password:
            if check_auth(username, password):
                user = _users.get(username)
                if user:
                    if flogin.login_user(user):
                        return flask.redirect(next or flask.url_for("user"))
                    error = 'Could not log in user.'
                else:
                    error = 'User not found.'
            else:
                error = 'Wrong username or password.'
        else:
            error = 'No username or password.'
        return flask.Response(
            'Could not verify your access level for that URL.\n {}'.format(error),
            401,
            {str('WWW-Authenticate'): str('Basic realm="Login Required"')})
    return flask.redirect(next or flask.url_for("user"))


@app.route('/user', methods=['GET'])
def user():
    '''Show information about the current user

    :rtype: A dictionary with the following keys
    :param id: User id
    :type id: int
    :param name: User name
    :type name: string
    :param is_active: Whether the user is currently active
    :type is_active: bool
    :param is_anonymous: The anonymous user is the default user if you
        are not logged in
    :type is_anonymous: bool
    '''
    user = flogin.current_user
    return flask.jsonify({
        'id': user.get_id(),
        'name': user.name,
        'is_active': user.is_active(),
        'is_anonymous': user.is_anonymous()
    })


@app.route('/logout')
def logout():
    """ Log out the active user
    """
    flogin.logout_user()
    next = flask.request.args.get('next')
    return flask.redirect(next or flask.url_for("user"))


@app.route("/job", methods=['GET'])
def job_list():
    '''List all jobs.

    :param _limit: maximum number of jobs to show (default 100)
    :type _limit: int
    :param _offset: how many jobs to skip before showin the first one (default 0)
    :type _offset: int
    :param _status: filter jobs by status (complete, error)
    :type _status: string

    Also, you can filter the jobs by their metadata. Use the metadata key
    as parameter key and the value as value.

    :rtype: A list of job ids
    '''
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


class DatetimeJsonEncoder(json.JSONEncoder):
    # Custon JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)


@app.route("/job/<job_id>", methods=['GET'])
def job_status(job_id, show_job_key=False, ignore_auth=False):
    '''Show a specific job.

    **Results:**

    :rtype: A dictionary with the following keys
    :param status: Status of job (complete, error)
    :type status: string
    :param sent_data: Input data for job
    :type sent_data: json encodable data
    :param job_id: An identifier for the job
    :type job_id: string
    :param result_url: Callback url
    :type result_url: url string
    :param data: Results from job.
    :type data: json encodable data
    :param error: Error raised during job execution
    :type error: string
    :param metadata: Metadata provided when submitting job.
    :type metadata: list of key - value pairs
    :param requested_timestamp: Time the job started
    :type requested_timestamp: timestamp
    :param finished_timestamp: Time the job finished
    :type finished_timestamp: timestamp

    :statuscode 200: no error
    :statuscode 403: not authorized to view the job's data
    :statuscode 404: job id not found
    :statuscode 409: an error occurred
    '''
    job_dict = get_job(job_id)
    if not job_dict:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    if not ignore_auth and not is_authorized(job_dict):
        return json.dumps({'error': 'not authorized'}), 403, headers
    job_dict.pop('api_key', None)
    if not show_job_key:
        job_dict.pop('job_key', None)
    return flask.Response(json.dumps(job_dict, cls=DatetimeJsonEncoder),
                          mimetype='application/json')


@app.route("/job/<job_id>", methods=['DELETE'])
def job_delete(job_id):
    '''Deletes the job together with its logs and metadata.

    :param job_id: An identifier for the job
    :type job_id: string

    :statuscode 200: no error
    :statuscode 403: not authorized to delete the job
    :statuscode 404: the job could not be found
    :statuscode 409: an error occurred
    '''
    conn = db.engine.connect()
    job = get_job(job_id)
    if not job:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    if not is_authorized(job):
        return json.dumps({'error': 'not authorized'}), 403, headers
    trans = conn.begin()
    try:
        conn.execute(db.jobs_table.delete().where(
                     db.jobs_table.c.job_id == job_id))
        trans.commit()
        return json.dumps({'success': True}), 200, headers
    except Exception, e:
        trans.rollback()
        return json.dumps({'error': str(e)}), 409, headers
    finally:
        conn.close()


@app.route("/job", methods=['DELETE'])
def clear_jobs():
    '''Clear old jobs

    :param days: Jobs for how many days should be kept (default: 10)
    :type days: integer

    :statuscode 200: no error
    :statuscode 403: not authorized to delete jobs
    :statuscode 409: an error occurred
    '''
    if not is_authorized():
        return json.dumps({'error': 'not authorized'}), 403, headers

    days = flask.request.args.get('days', None)
    return _clear_jobs(days)


def _clear_jobs(days=None):
    if days is None:
        days = app.config.get('KEEP_JOBS_AGE')
    else:
        try:
            days = int(days)
        except Exception as e:
            return json.dumps({'error': str(e)}), 409, headers
    conn = db.engine.connect()
    trans = conn.begin()
    date = datetime.datetime.now() - datetime.timedelta(days=days)
    try:
        conn.execute(db.jobs_table.delete().where(
                     db.jobs_table.c.finished_timestamp < date))
        trans.commit()
        return json.dumps({'success': True}), 200, headers
    except Exception as e:
        trans.rollback()
        return json.dumps({'error': str(e)}), 409, headers
    finally:
        conn.close()


@app.route("/job/<job_id>/data", methods=['GET'])
def job_data(job_id):
    '''Get the raw data that the job returned. The mimetype
    will be the value provided in the metdata for the key ``mimetype``.

    **Results:**

    :rtype: string

    :statuscode 200: no error
    :statuscode 403: not authorized to view the job's data
    :statuscode 404: job id not found
    :statuscode 409: an error occurred
    '''
    job_dict = get_job(job_id)
    if not job_dict:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    if not is_authorized(job_dict):
        return json.dumps({'error': 'not authorized'}), 403, headers
    if job_dict['error']:
        return json.dumps({'error': job_dict['error']}), 409, headers
    content_type = job_dict['metadata'].get('mimetype')
    return flask.Response(job_dict['data'], mimetype=content_type)


@app.route("/job/<job_id>", methods=['POST'])
@app.route("/job", methods=['POST'])
def job(job_id=None):
    '''Submit a job. If no id is provided, a random id will be generated.

    :param job_type: Which kind of job should be run. Has to be one of the
        available job types.
    :type job_type: string
    :param api_key: An API key that is needed to execute the job. This could
        be a CKAN API key that is needed to write any data. The key will also be
        used to administer jobs. If you don't want to use a real API key, you can
        provide a random string that you keep secure.
    :type api_key: string
    :param data: Data that is send to the job as input. (Optional)
    :type data: json encodable data
    :param result_url: Callback url that is called once the job has finished.
        (Optional)
    :type result_url: url string
    :param metadata: Data needed for the execution of the job which is not
        the input data. (Optional)
    :type metadata: list of key - value pairs

    **Results:**

    :rtype: A dictionary with the following keys
    :param job_id: An identifier for the job
    :type job_id: string
    :param job_key: A key that is required to view and administer the job
    :type job_key: string

    :statuscode 200: no error
    :statuscode 409: an error occurred

    '''
    if not job_id:
        job_id = str(uuid.uuid4())

    # key required for job administration
    job_key = str(uuid.uuid4())

    ############# ERROR CHECKING ################
    try:
        input = flask.request.json
    except werkzeug.exceptions.BadRequest:
        return json.dumps({"error": "Malformed json"}), 409, headers

    # Idk why but this is needed for some libraries that
    # send malformed content types
    if (not input and
            'application/json' in flask.request.content_type.lower()):
        try:
            input = json.loads(flask.request.data)
        except ValueError:
            pass
    if not input:
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

    api_key = input.get('api_key')
    if not api_key:
        return json.dumps({"error": "Please provide your API key."}), 409, headers

    metadata = input.get('metadata', {})
    if not isinstance(metadata, dict):
        return json.dumps({"error": "metadata has to be a json object"}), \
            409, headers
    ############# END CHECKING ################

    synchronous_job = sync_types.get(job_type)
    if synchronous_job:
        return run_synchronous_job(synchronous_job, job_id, job_key, input)
    else:
        asynchronous_job = async_types.get(job_type)
        return run_asynchronous_job(asynchronous_job, job_id, job_key, input)


def run_synchronous_job(job, job_id, job_key, input):
    try:
        store_job(job_id, job_key, input)
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
    finally:
        update_dict['api_key'] = None
        update_dict['finished_timestamp'] = datetime.datetime.now()

        api_key = get_job(job_id)['api_key']
        update_job(job_id, update_dict)
    result_ok = send_result(job_id, api_key)

    if not result_ok:
        update_dict['error'] = json.dumps('Process completed but unable to '
                                          'post to result_url')
        update_job(job_id, update_dict)

    return job_status(job_id=job_id, show_job_key=True, ignore_auth=True)


def run_asynchronous_job(job, job_id, job_key, input):
    if not scheduler.running:
        scheduler.start()
    try:
        store_job(job_id, job_key, input)
    except sa.exc.IntegrityError:
        error_string = 'job_id {} already exists'.format(job_id)
        return json.dumps({"error": error_string}), 409, headers

    scheduler.add_job(RunNowTrigger(), job, [job_id, input], None)

    return job_status(job_id=job_id, show_job_key=True, ignore_auth=True)


def store_job(job_id, job_key, input):
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
            api_key=input.get('api_key')),
            job_key=job_key
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
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()


def is_authorized(job=None):
    '''Returns true if the request is authorized for the job
    if provided. If no job is provided, the user has to be admin
    to be authorized.
    '''
    if flogin.current_user.is_authenticated():
        return True
    if job:
        job_key = flask.request.headers.get('Authorization')
        if job_key == app.config.get('SECRET_KEY'):
            return True
        return job['job_key'] == job_key
    return False


def update_job(job_id, update_dict):
    db.engine.execute(db.jobs_table.update()
                      .where(db.jobs_table.c.job_id == job_id)
                      .values(**update_dict))


def send_result(job_id, api_key=None):
    ''' Send results to where requested.

    If api_key is provided, it is used, otherwiese
    the key from the job will be used.
    '''
    job_dict = get_job(job_id)
    result_url = job_dict.get('result_url')
    if not result_url:
        return True
    api_key_from_job = job_dict.pop('api_key', None)
    if not api_key:
        api_key = api_key_from_job
    headers = {'Content-Type': 'application/json'}
    if api_key:
        if ':' in api_key:
            header, key = api_key.split(':')
        else:
            header, key = 'Authorization', api_key
        headers[header] = key

    try:
        result = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=DatetimeJsonEncoder),
            headers=headers)
    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok


def get_job(job_id):
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
    result_dict['logs'] = get_logs(job_id)
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


def get_logs(job_id):
    results = db.engine.execute(db.logs_table.select()
                                .where(db.logs_table.c.job_id ==
                                       job_id)).fetchall()
    results = map(dict, results)

    def remove_job_id(d):
        d.pop('job_id')
        return d
    return map(remove_job_id, results)


if __name__ == '__main__':
    configure()
    app.run(app.config.get('HOST'), app.config.get('PORT'))
