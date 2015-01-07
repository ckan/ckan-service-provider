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
import werkzeug
import apscheduler.scheduler as apscheduler
import apscheduler.events as events
import apscheduler.jobstores.sqlalchemy_store as sqlalchemy_store
import sqlalchemy.sql as sql
import sqlalchemy as sa
import requests
from werkzeug.contrib.fixers import ProxyFix

import ckanserviceprovider.db as db
import ckanserviceprovider.util as util
import ckanserviceprovider.default_settings as default_settings


# A global variable that's set to a test URL during testing.
# Only used for tests.
_TEST_CALLBACK_URL = None


class User(flogin.UserMixin):
    def __init__(self, name, id, active=True):
        self.name = name
        self.id = id
        self.active = active

    def is_active(self):
        return self.active


class Anonymous(flogin.AnonymousUserMixin):
    name = u"Anonymous"


class RunNowTrigger(object):
    '''Custom apscheduler trigger to run job once and only once.'''
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

    if event.code == events.EVENT_JOB_MISSED:
        db.mark_job_as_missed(job_id)
    elif event.exception:
        if isinstance(event.exception, util.JobError):
            error_object = event.exception.as_dict()
        else:
            error_object = "\n".join(traceback.format_tb(event.traceback) +
                    [repr(event.exception)])
        db.mark_job_as_errored(job_id, error_object)
    else:
        db.mark_job_as_completed(job_id, event.retval)

    job_dict = db.get_job(job_id)
    api_key = job_dict["api_key"]
    result_ok = send_result(job_id, api_key)

    if not result_ok:
        db.mark_job_as_failed_to_post_result(job_id)

    # Optionally notify tests that job_listener() has finished.
    if _TEST_CALLBACK_URL:
        requests.get(_TEST_CALLBACK_URL)


headers = {str('Content-Type'): str('application/json')}

CKANSERVICEPROVIDER_BLUEPRINT = flask.Blueprint(
    "ckanserviceprovider", __name__)


@CKANSERVICEPROVIDER_BLUEPRINT.route("/", methods=['GET'])
def index():
    '''Show link to documentation.

    :rtype: A dictionary with the following keys
    :param help: Help text
    :type help: string
    '''
    return flask.jsonify(
        help="Get help at http://ckan-service-provider.readthedocs.org/"
    )


@CKANSERVICEPROVIDER_BLUEPRINT.route("/status", methods=['GET'])
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
    job_types = flask.current_app.job_types

    counts = {}
    for job_status_ in flask.current_app.JOB_STATUSES:
        counts[job_status_] = db.ENGINE.execute(
            db.JOBS_TABLE.count()
            .where(db.JOBS_TABLE.c.status == job_status_)
        ).first()[0]

    return flask.jsonify(
        version=0.1,
        job_types=job_types,
        name=flask.current_app.config.get('NAME', 'example'),
        stats=counts
    )


def check_auth(username, password):
    '''This function is called to check if a username /
    password combination is valid.
    '''
    return (username == flask.current_app.config['USERNAME'] and
            password == flask.current_app.config['PASSWORD'])


@CKANSERVICEPROVIDER_BLUEPRINT.route('/login', methods=['POST', 'GET'])
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
                user = flask.current_app.users.get(username)
                if user:
                    if flogin.login_user(user):
                        return flask.redirect(next or flask.url_for(
                            "ckanserviceprovider.user"))
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
    return flask.redirect(next or flask.url_for("ckanserviceprovider.user"))


@CKANSERVICEPROVIDER_BLUEPRINT.route('/user', methods=['GET'])
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


@CKANSERVICEPROVIDER_BLUEPRINT.route('/logout')
def logout():
    """ Log out the active user
    """
    flogin.logout_user()
    next = flask.request.args.get('next')
    return flask.redirect(next or flask.url_for("ckanserviceprovider.user"))


@CKANSERVICEPROVIDER_BLUEPRINT.route("/job", methods=['GET'])
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
        [db.JOBS_TABLE.c.job_id],
        from_obj=[db.JOBS_TABLE.outerjoin(
            db.METADATA_TABLE,
            db.JOBS_TABLE.c.job_id == db.METADATA_TABLE.c.job_id)
        ]).\
        group_by(db.JOBS_TABLE.c.job_id).\
        order_by(db.JOBS_TABLE.c.requested_timestamp.desc()).\
        limit(limit).offset(offset)

    status = args.pop('_status', None)
    if status:
        select = select.where(db.JOBS_TABLE.c.status == status)

    ors = []
    for key, value in args.iteritems():
        # Turn strings into unicode to stop SQLAlchemy
        # "Unicode type received non-unicode bind param value" warnings.
        key = unicode(key)

        ors.append(sql.and_(db.METADATA_TABLE.c.key == key,
                   db.METADATA_TABLE.c.value == value))

    if ors:
        select = select.where(sql.or_(*ors))
        select = select.having(
            sql.func.count(db.JOBS_TABLE.c.job_id) == len(ors)
        )

    result = db.ENGINE.execute(select)
    listing = []
    for (job_id,) in result:
        listing.append(
            flask.url_for('ckanserviceprovider.job_status', job_id=job_id))

    return flask.jsonify(list=listing)


class DatetimeJsonEncoder(json.JSONEncoder):
    # Custon JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)


@CKANSERVICEPROVIDER_BLUEPRINT.route("/job/<job_id>", methods=['GET'])
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
    job_dict = db.get_job(job_id)
    if not job_dict:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    if not ignore_auth and not is_authorized(job_dict):
        return json.dumps({'error': 'not authorized'}), 403, headers
    job_dict.pop('api_key', None)
    if not show_job_key:
        job_dict.pop('job_key', None)
    return flask.Response(json.dumps(job_dict, cls=DatetimeJsonEncoder),
                          mimetype='application/json')


@CKANSERVICEPROVIDER_BLUEPRINT.route("/job/<job_id>", methods=['DELETE'])
def job_delete(job_id):
    '''Deletes the job together with its logs and metadata.

    :param job_id: An identifier for the job
    :type job_id: string

    :statuscode 200: no error
    :statuscode 403: not authorized to delete the job
    :statuscode 404: the job could not be found
    :statuscode 409: an error occurred
    '''
    conn = db.ENGINE.connect()
    job = db.get_job(job_id)
    if not job:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    if not is_authorized(job):
        return json.dumps({'error': 'not authorized'}), 403, headers
    trans = conn.begin()
    try:
        conn.execute(db.JOBS_TABLE.delete().where(
                     db.JOBS_TABLE.c.job_id == job_id))
        trans.commit()
        return json.dumps({'success': True}), 200, headers
    except Exception, e:
        trans.rollback()
        return json.dumps({'error': str(e)}), 409, headers
    finally:
        conn.close()


@CKANSERVICEPROVIDER_BLUEPRINT.route("/job", methods=['DELETE'])
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
        days = flask.current_app.config.get('KEEP_JOBS_AGE')
    else:
        try:
            days = int(days)
        except Exception as e:
            return json.dumps({'error': str(e)}), 409, headers
    conn = db.ENGINE.connect()
    trans = conn.begin()
    date = datetime.datetime.now() - datetime.timedelta(days=days)
    try:
        conn.execute(db.JOBS_TABLE.delete().where(
                     db.JOBS_TABLE.c.finished_timestamp < date))
        trans.commit()
        return json.dumps({'success': True}), 200, headers
    except Exception as e:
        trans.rollback()
        return json.dumps({'error': str(e)}), 409, headers
    finally:
        conn.close()


@CKANSERVICEPROVIDER_BLUEPRINT.route("/job/<job_id>/data", methods=['GET'])
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
    job_dict = db.get_job(job_id)
    if not job_dict:
        return json.dumps({'error': 'job_id not found'}), 404, headers
    if not is_authorized(job_dict):
        return json.dumps({'error': 'not authorized'}), 403, headers
    if job_dict['error']:
        return json.dumps({'error': job_dict['error']}), 409, headers
    content_type = job_dict['metadata'].get('mimetype')
    return flask.Response(job_dict['data'], mimetype=content_type)


@CKANSERVICEPROVIDER_BLUEPRINT.route("/job/<job_id>", methods=['POST'])
@CKANSERVICEPROVIDER_BLUEPRINT.route("/job", methods=['POST'])
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

    job_types = flask.current_app.job_types

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

    synchronous_job = flask.current_app.get_synchronous_job(job_type)
    if synchronous_job:
        return run_synchronous_job(synchronous_job, job_id, job_key, input)
    else:
        asynchronous_job = flask.current_app.get_asynchronous_job(job_type)
        return run_asynchronous_job(asynchronous_job, job_id, job_key, input)


def run_synchronous_job(job, job_id, job_key, input):
    try:
        db.add_pending_job(job_id, job_key, **input)
    except sa.exc.IntegrityError, e:
        error_string = 'job_id {} already exists'.format(job_id)
        return json.dumps({"error": error_string}), 409, headers

    try:
        result = job(job_id, input)

        if hasattr(result, "__call__"):
            db.mark_job_as_completed(job_id)
            return flask.Response(result(), mimetype='application/json')
        else:
            db.mark_job_as_completed(job_id, result)

    except util.JobError, e:
        db.mark_job_as_errored(job_id, e.as_dict())
    except Exception, e:
        db.mark_job_as_errored(
            job_id, traceback.format_tb(sys.exc_traceback)[-1] + repr(e))

    api_key = db.get_job(job_id)['api_key']
    result_ok = send_result(job_id, api_key)

    if not result_ok:
        db.mark_job_as_failed_to_post_result(job_id)

    return job_status(job_id=job_id, show_job_key=True, ignore_auth=True)


def run_asynchronous_job(job, job_id, job_key, input):
    flask.current_app.start_scheduler()
    try:
        db.add_pending_job(job_id, job_key, **input)
    except sa.exc.IntegrityError:
        error_string = 'job_id {} already exists'.format(job_id)
        return json.dumps({"error": error_string}), 409, headers

    flask.current_app.schedule_job(job, job_id, input)

    return job_status(job_id=job_id, show_job_key=True, ignore_auth=True)


def is_authorized(job=None):
    '''Returns true if the request is authorized for the job
    if provided. If no job is provided, the user has to be admin
    to be authorized.
    '''
    if flogin.current_user.is_authenticated():
        return True
    if job:
        job_key = flask.request.headers.get('Authorization')
        if job_key == flask.current_app.config.get('SECRET_KEY'):
            return True
        return job['job_key'] == job_key
    return False


def send_result(job_id, api_key=None):
    ''' Send results to where requested.

    If api_key is provided, it is used, otherwiese
    the key from the job will be used.
    '''
    job_dict = db.get_job(job_id)
    result_url = job_dict.get('result_url')

    if not result_url:

        # A job with an API key (for using when posting to the callback URL)
        # but no callback URL is weird, but it can happen.
        db.delete_api_key(job_id)

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

        db.delete_api_key(job_id)

    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok


def _configure_app(app):
    """Configure the given flask.Flask app for CKAN Service Provider."""
    app.url_map.strict_slashes = False
    app.config.from_object(default_settings)
    app.config.from_envvar('JOB_CONFIG', silent=True)
    if not app.config.get('SQLALCHEMY_DATABASE_URI'):
        raise Exception('No db_url in config')
    app.wsgi_app = ProxyFix(app.wsgi_app)


def _configure_logger_for_production(app, logger):
    """Configure the given logger for production deployment.

    Logs to stderr and file, and emails errors to admins.

    """
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.INFO)
    if 'STDERR' in app.config:
        logger.addHandler(stderr_handler)

    file_handler = logging.handlers.RotatingFileHandler(
        app.config.get('LOG_FILE'), maxBytes=67108864, backupCount=5)
    file_handler.setLevel(logging.INFO)
    if 'LOG_FILE' in app.config:
        logger.addHandler(file_handler)

    mail_handler = logging.handlers.SMTPHandler(
        '127.0.0.1',
        app.config.get('FROM_EMAIL'),
        app.config.get('ADMINS', []),
        'CKAN Service Error')
    mail_handler.setLevel(logging.ERROR)
    if 'FROM_EMAIL' in app.config:
        logger.addHandler(mail_handler)


def _configure_logger_for_debugging(app, logger):
    """Configure the given logger for debug mode."""
    logger.addHandler(app.logger.handlers[0])


def _configure_logger(app):
    """Configure the logging module."""
    if not app.debug:
        _configure_logger_for_production(app, logging.getLogger())
    elif not app.testing:
        _configure_logger_for_debugging(app, logging.getLogger())


def _init_login_manager(app):
    """Initialise and configure the login manager."""
    login_manager = flogin.LoginManager()
    login_manager.setup_app(app)
    login_manager.anonymous_user = Anonymous
    login_manager.login_view = "login"

    users = {app.config['USERNAME']: User('Admin', 0)}
    names = dict((int(v.get_id()), k) for k, v in users.items())

    @login_manager.user_loader
    def load_user(userid):
        userid = int(userid)
        name = names.get(userid)
        return users.get(name)

    return users


def _init_scheduler(db_uri):
    """Return a new initialised and configured apscheduler Scheduler object."""
    scheduler = apscheduler.Scheduler()
    scheduler.misfire_grace_time = 3600
    scheduler.add_jobstore(
        sqlalchemy_store.SQLAlchemyJobStore(url=db_uri), 'default')
    scheduler.add_listener(
        job_listener,
        events.EVENT_JOB_EXECUTED | events.EVENT_JOB_MISSED
        | events.EVENT_JOB_ERROR)
    return scheduler


class CKANServiceProvider(flask.Flask):

    """The CKAN Service Provider WSGI application and central object."""

    def __init__(self, name):
        """Return a new CKAN Service Provider app object.

        This returns a "blank" CKAN Service Provider app with no job functions
        registered. You should use the app's methods to register your job
        functions with it before calling app.run(). For example:

            app = ckanserviceprovider.web.CKANServiceProvider(__name__)

            app.register_synchronous_jobs(my_job_function)
            app.register_asynchronous_jobs(my_other_job_function)

            app.run()

        """
        super(CKANServiceProvider, self).__init__(name)

        self._sync_types = {}
        self._async_types = {}
        self.JOB_STATUSES = ['pending', 'complete', 'error']

        self.register_blueprint(CKANSERVICEPROVIDER_BLUEPRINT)

        _configure_app(self)
        _configure_logger(self)

        self._scheduler = _init_scheduler(
            self.config.get('SQLALCHEMY_DATABASE_URI'))

        db.init(self.config.get('SQLALCHEMY_DATABASE_URI'))

        self.users = _init_login_manager(self)

        if "_TEST_CALLBACK_URL" in self.config:
            global _TEST_CALLBACK_URL
            _TEST_CALLBACK_URL = self.config["_TEST_CALLBACK_URL"]

    def register_synchronous_jobs(self, *functions):
        """Register one or more synchronous job functions.

        The functions will be made available via the CKAN Service Provider API
        as synchronous job types.

        """
        for func in functions:
            self._sync_types[func.__name__] = func

    def register_asynchronous_jobs(self, *functions):
        """Register one or more asynchronous job functions.

        The functions will be made available via the CKAN Service Provider API
        as asynchronous job types.

        """
        for func in functions:
            self._async_types[func.__name__] = func

    @property
    def synchronous_job_types(self):
        """A list of the currently registered synchronous job types."""
        return self._sync_types.keys()

    @property
    def asynchronous_job_types(self):
        """A list of the currently registered asynchronous job types."""
        return self._async_types.keys()

    @property
    def job_types(self):
        """A list of the registered asynchronous and synchronous job types."""
        return self.asynchronous_job_types + self.synchronous_job_types

    def get_synchronous_job(self, job_type):
        """Return the Python function for the given synchronous job type."""
        return self._sync_types.get(job_type)

    def get_asynchronous_job(self, job_type):
        """Return the Python function for the given asynchronous job type."""
        return self._async_types.get(job_type)

    def start_scheduler(self):
        """Start the scheduler if it isn't already running."""
        if not self._scheduler.running:
            self._scheduler.start()

    def schedule_job(self, job, job_id, input_data):
        """Schedule a job to be run asynchronously."""
        self._scheduler.add_job(
            RunNowTrigger(), job, [job_id, input_data], None)

    def run(self):
        """Run the CKANServiceProvider app in a local development server."""
        super(CKANServiceProvider, self).run(
            self.config.get('HOST'), self.config.get('PORT'))


def main():
    """Run a blank CKAN Service Provider app in a development web server.

    The API will be there but there won't be any job functions to run. This is
    pointless but may be handy for testing.

    """
    CKANServiceProvider(__name__).run()


if __name__ == '__main__':
    main()
