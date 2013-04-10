import uuid
import datetime
import sys
import json
import traceback

import flask
import flask.ext.login as flogin
#from flask.ext.admin import Admin
import werkzeug
import apscheduler.scheduler as apscheduler
import apscheduler.events as events
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

app = flask.Flask(__name__)
scheduler = apscheduler.Scheduler()
#Allow a day for jobs to be run otherwise drop them. Should rerun these later.
scheduler.misfire_grace_time = 3600


class User(flogin.UserMixin):
    def __init__(self, name, id, active=True):
        self.name = name
        self.id = id
        self.active = active

    def is_active(self):
        return self.active


class Anonymous(flogin.AnonymousUser):
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

    #Admin(app)


class RunNowTrigger(object):
    ''' custom apscheduler trigger to run job once and only
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
    """Show link to documentation.

    :rtype: A dictionary with the following keys
    :param help: Help text
    :type help: string
    """
    return flask.jsonify(
        help="""
        Get help at:
        http://ckan-service-provider.readthedocs.org/."""
    )


@app.route("/status", methods=['GET'])
def status():
    """Show version, available job types and name of service.

    **Results:**

    :rtype: A dictionary with the following keys
    :param version: Version of the service provider
    :type version: float
    :param job_types: Available job types
    :type job_types: list of strings
    :param name: Name of the service
    :type name: string
    """
    job_types = async_types.keys() + sync_types.keys()
    return flask.jsonify(
        version=0.1,
        job_types=job_types,
        name=app.config.get('NAME', 'example')
    )


def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return (username == app.config['USERNAME'] and
            password == app.config['PASSWORD'])


@app.route('/login', methods=['POST', 'GET'])
def login():
    """ Log in as administrator

    You can use wither basic auth or form based login (via POST).

    :param username: The administrator's username
    :type username: string
    :param password: The administrator's password
    :type password: string
    """
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
            {'WWW-Authenticate': 'Basic realm="Login Required"'})
    return flask.redirect(next or flask.url_for("user"))


@app.route('/user', methods=['GET'])
def user():
    """ Show information about the active user

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
    """
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
    """List all jobs.

    :rtype: A list of job ids
    """
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
    """Show a specific job.

    **Results:**

    :rtype: A dictionary with the following keys
    :param status: Status of job (complete, error, running)
    :type status: string
    :param sent_data: Input data for job
    :type sent_data: json encodable data
    :param job_id: Id of job
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
    :statuscode 404: job id not found
    :statuscode 409: an error occurred
    """
    job_status = get_job_status(job_id)
    if not job_status:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    job_status.pop('api_key', None)
    return flask.jsonify(job_status)


@app.route("/job/<job_id>/data", methods=['GET'])
def job_data(job_id):
    """Get the raw data that the job returned. The mimetype
    will be the value provided in the metdata for the key ``mimetype``.

    **Results:**

    :rtype: string

    :statuscode 200: no error
    :statuscode 404: job id not found
    :statuscode 409: an error occurred
    """
    job_status = get_job_status(job_id)
    if not job_status:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    if job_status['error']:
        return json.dumps({'error': job_status['error']}), 409, headers
    content_type = job_status['metadata'].get('mimetype')
    return flask.Response(job_status['data'], mimetype=content_type)


@app.route("/job/<job_id>/resubmit", methods=['POST'])
@flogin.login_required
def resubmit_job(job_id):
    """Resubmit a job that failed.

    **Results:**

    See :http:post:`/job/<job_id>`

    :statuscode 200: no error
    :statuscode 404: job id not found
    :statuscode 409: an error occurred
    """
    conn = db.engine.connect()
    job = conn.execute(db.jobs_table.select().where(
                       db.jobs_table.c.job_id == job_id)).first()
    if not job:
        return json.dumps({"error": ('job_id not found')}), 404, headers
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
    """Sumbit a job. If no id is provided, a random id will be generated.

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

    See :http:get:`/job/<job_id>`

    :statuscode 200: no error
    :statuscode 409: an error occurred

    """
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

    api_key = input.get('api_key')
    if not api_key:
        return json.dumps({"error": "Please provide your API key or a random "
                           "string that you keep secure."}), 409, headers

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

app.wsgi_app = ProxyFix(app.wsgi_app)


def run():
    return app.run(port=int(app.config.get('PORT', 5000)))


def test_client():
    return app.test_client()
