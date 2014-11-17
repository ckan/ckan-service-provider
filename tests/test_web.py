import os
import subprocess
import json
import time
import logging
import uuid
from nose.tools import assert_equal

import requests
import ckanserviceprovider.web as web
import ckanserviceprovider.job as job
import ckanserviceprovider.util as util
import ckanserviceprovider.db as db


def configure():
    """Configure the Flask app.

    This has to be called just once per test run (not e.g. once for each test).

    """
    os.environ['JOB_CONFIG'] = os.path.join(
        os.path.dirname(__file__), 'settings_test.py')
    web.init()
configure()


def reset_db():
    """Reset the database and scheduler.

    Should be called after each test.

    """
    web.scheduler.shutdown(wait=True)
    db.drop_all()
    db.init(web.app.config.get('SQLALCHEMY_DATABASE_URI'))
    web.init_scheduler(web.app.config.get('SQLALCHEMY_DATABASE_URI'))


def test_client():
    """Return a test client for the ckanserviceprovider web app."""
    return web.app.test_client()


def login(app, username='testadmin', password='testpass'):
    return app.post('/login', data=dict(
        username=username,
        password=password
    ), follow_redirects=True)


@job.sync
def echo(task_id, input):
    if input['data'].startswith('>'):
        raise util.JobError('Do not start message with >')
    if input['data'].startswith('#'):
        raise Exception('Something went totally wrong')
    #if input['data'].startswith('&'):
    #    util.logger.warn('Just a warning')
    return '>' + input['data']


@job.sync
def echo_raw(task_id, input):
    if input['data'].startswith('>'):
        raise util.JobError('Do not start message with >')

    def raw():
        for x in sorted(input['data']):
            yield x

    return raw


@job.async
def example(task_id, input):
    if 'time' not in input['data']:
        raise util.JobError('time not in input')

    time.sleep(input['data']['time'])
    return 'Slept for ' + str(input['data']['time']) + ' seconds.'


@job.async
def failing(task_id, input):
    time.sleep(0.1)
    raise util.JobError('failed')


@job.async
def log(task_id, input):
    handler = util.StoringHandler(task_id, input)
    logger = logging.Logger(task_id)
    logger.addHandler(handler)

    logger.warn('Just a warning')


class TestWeb():

    @classmethod
    def setup_class(cls):
        fake_ckan_path = os.path.join(os.path.dirname(__file__),
                                      "fake_ckan.py")
        cls.fake_ckan = subprocess.Popen(['python', fake_ckan_path],
                                         shell=False)
        #make sure service is running
        for i in range(0, 10):
            time.sleep(1)
            response1 = requests.get('http://0.0.0.0:9091/')
            if not response1:
                continue
            return
        cls.fake_ckan.kill()
        raise Exception('services did not start!')

    @classmethod
    def teardown_class(cls):
        cls.fake_ckan.kill()

    def teardown(self):
        reset_db()

    def test_status(self):
        '''/status should return JSON with the app version, job types, etc.'''
        app = test_client()
        rv = app.get('/status')
        status_data = json.loads(rv.data)
        status_data.pop('stats')
        assert_equal(status_data, dict(version=0.1,
                                       job_types=['failing',
                                                  'example',
                                                  'log',
                                                  'echo_raw',
                                                  'echo'],
                                       name='testing'))

    def test_content_type(self):
        '''Pages should have content_type "application/json".'''
        app = test_client()
        # make sure that we get json
        for page in ['/job', '/status', '/job/foo']:
            rv = app.get(page)
            assert_equal(rv.content_type, 'application/json')

    def test_bad_post(self):
        '''Invalid posts to /job should receive error messages in JSON.'''
        app = test_client()
        rv = app.post('/job', data='{"ffsfsafsa":"moo"}')
        assert_equal(json.loads(rv.data), {u'error': u'Not recognised as json,'
                                           ' make sure content type'
                                           ' is application/json'})

        rv = app.post('/job',
                      data='{"ffsfsafsa":moo}',
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Malformed json'})

        rv = app.post('/job',
                      data=json.dumps({
                          "api_key": 42,
                          "data": {"time": 5}}),
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Please specify a job '
                                           'type'})

        rv = app.post('/job',
                      data=json.dumps({"job_type": "moo",
                                       "api_key": 42,
                                       "data": {"time": 5}}),
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Job type moo not available.'
                                           ' Available job types are '
                                           'failing, example, log, echo_raw, echo'})

        rv = app.post('/job',
                      data=json.dumps({"job_type": "example",
                                       "data": {"time": 5}}),
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Please provide your API key.'})

        rv = app.post('/job',
                      data=json.dumps({"job_type": "example",
                                       "api_key": 42,
                                       "data": {"time": 5},
                                       "foo": 42}),
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Too many arguments. '
                                           'Extra keys are foo'})

    def test_asynchronous_post_with_good_job(self):
        '''A valid post to /job should get back a JSON object with a job ID.

        Also tests a bunch of other stuff about /job.

        '''
        app = test_client()
        # good job
        rv = app.post('/job',
                      data=json.dumps({"job_type": "example",
                                       "api_key": 42,
                                       "data": {"time": 0.1}}),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        assert 'job_id' in return_data

    def test_get_job_does_not_return_api_key(self):
        '''The dict that get_job() returns should not contain the API key.'''
        app = test_client()

        response = app.post(
            '/job',
            data=json.dumps(
                {"job_type": "example",
                 "api_key": 42,
                 "data": {"time": 0.1}}),
            content_type='application/json')
        return_data = json.loads(response.data)

        # FIXME: Need a better way to wait for an asynchronous task to finish.
        time.sleep(2)

        job = web.get_job(return_data['job_id'])
        assert not job['api_key'], job

    def test_post_job_with_custom_id(self):
        '''Posting a job with a custom ID should return the ID in the JSON.'''
        app = test_client()

        # good job with name
        rv = app.post('/job/moo',
                      data=json.dumps({"job_type": "example",
                                       "api_key": 42,
                                       "data": {"time": 0.1}}),
                      content_type='application/json')

        assert json.loads(rv.data)['job_id'] == "moo", json.loads(rv.data)

    def test_get_job_while_pending(self):
        '''Create a job with a custom ID and get the job while still pending.

        This tests the value of the job's metadata while the job is still in a
        pending status.

        '''
        app = test_client()
        app.post(
            '/job/moo',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": 1}}),
            content_type='application/json')

        login(app)
        rv = app.get('/job/moo')

        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        assert job_status_data == {u'status': u'pending',
                                   u'sent_data': {"time": 1},
                                   u'job_id': u'moo',
                                   u'finished_timestamp': None,
                                   u'job_type': u'example',
                                   u'error': None,
                                   u'data': None,
                                   u'metadata': {},
                                   u'logs': [],
                                   u'result_url': None}, job_status_data

    def test_get_job_when_completed(self):
        '''Get a job with a custom ID after it has completed.

        Tests the value of the job's metadata after the job has completed.

        '''
        app = test_client()
        app.post(
            '/job/moo',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": 0.1}}),
            content_type='application/json')

        time.sleep(2)

        login(app)

        rv = app.get('/job/moo')

        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')

        assert job_status_data == {u'status': u'complete',
                                   u'sent_data': {"time": 0.1},
                                   u'job_id': u'moo',
                                   u'job_type': u'example',
                                   u'error': None, u'data':
                                   u'Slept for 0.1 seconds.',
                                   u'metadata': {},
                                   u'logs': [],
                                   u'result_url': None}, job_status_data

    def test_post_job_with_duplicate_custom_id(self):
        '''Posting a job with a duplicate ID should error.'''
        app = test_client()
        app.post(
            '/job/moo',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": 0.1}}),
            content_type='application/json')

        rv = app.post(
            '/job/moo',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": 0.1}}),
            content_type='application/json')

        assert json.loads(rv.data) == {u'error': u'job_id moo '
                                                 'already exists'}, \
            json.loads(rv.data)

    def test_post_with_job_error(self):
        '''If a job raises JobError then ... the response should contain the job id???'''
        # The 'example' job type (defined above) will raise JobError for this
        # data because the data has no "time" key.
        app = test_client()
        rv = app.post(
            '/job/missing_time',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {}}),
            content_type='application/json')

        assert json.loads(rv.data)['job_id'] == "missing_time", \
            json.loads(rv.data)

    def test_post_with_job_exception(self):
        '''If a job raises an exception the HTTP response should have an error.

        If a job raises an arbitrary exception (e.g. because of a mistake in
        the job code) the response to the job post HTTP request should have
        "exception" instead of the job ID.

        '''
        app = test_client()
        # The 'example' job type (defined above) will crash on this invalid
        # time value.
        rv = app.post(
            '/job/exception',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": "not_a_time"}}),
            content_type='application/json')

        assert json.loads(rv.data)['job_id'] == "exception", \
            json.loads(rv.data)

    def test_get_job_with_known_error(self):
        '''Test getting a job that failed with a JobError.

        Among other things, we expect the job dict to have an "error" key with
        the error string from the job function as its value.

        '''
        app = test_client()
        rv = app.post(
            '/job/missing_time',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {}}),
            content_type='application/json')

        login(app)

        time.sleep(2)

        rv = app.get('/job/missing_time')

        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')

        assert job_status_data == {u'status': u'error',
                                   u'sent_data': {},
                                   u'job_id': u'missing_time',
                                   u'job_type': u'example',
                                   u'error': u'time not in input',
                                   u'data': None,
                                   u'metadata': {},
                                   u'logs': [],
                                   u'result_url': None}, job_status_data

        # get_job() shouldn't return the API key, either.
        job = web.get_job(job_status_data['job_id'])
        assert not job['api_key'], job

    def test_get_job_with_unknown_error(self):
        '''Test getting a job that failed with a random exception.

        A random exception type caused by an error in the job function code,
        as opposed to a deliberately raised JobError.

        '''
        app = test_client()
        rv = app.post(
            '/job/exception',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": "not_a_time"}}),
            content_type='application/json')

        login(app)

        time.sleep(2)

        rv = app.get('/job/exception')

        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        error = job_status_data.pop('error')

        assert job_status_data == {u'status': u'error',
                                   u'sent_data': {"time": "not_a_time"},
                                   u'job_id': u'exception',
                                   u'job_type': u'example',
                                   u'data': None,
                                   u'metadata': {},
                                   u'logs': [],
                                   u'result_url': None}, job_status_data
        assert 'TypeError' in error[-1], error

        # get_job() shouldn't return the API key, either.
        job = web.get_job(job_status_data['job_id'])
        assert not job['api_key'], job

    def test_asynchronous_post_with_return_url(self):
        '''Test posting asynchronous jobs and getting the results.

        Tests both jobs with successful results and jobs with bad results.

        '''
        app = test_client()
        rv = app.post(
            '/job/with_result',
            data=json.dumps({"job_type": "example",
                             "api_key": 42,
                             "data": {"time": 0.1},
                             "metadata": {'key': 'value'},
                             "result_url": "http://0.0.0.0:9091/result",
                             "api_key": "header:key"}),
            content_type='application/json')

        # bad result
        rv = app.post(
            '/job/with_bad_result',
            data=json.dumps({"job_type": "example",
                             "api_key": 42,
                             "data": {"time": 0.1},
                             "metadata": {'key': 'value'},
                             "result_url": "http://0.0.0.0:9091/resul"}),
            content_type='application/json')

        time.sleep(0.5)

        login(app)
        rv = app.get('/job/with_bad_result')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        assert job_status_data == {u'status': u'complete',
                                   u'sent_data': {u'time': 0.1},
                                   u'job_id': u'with_bad_result',
                                   u'job_type': u'example',
                                   u'error': u'Process completed but'
                                             ' unable to post to result_url',
                                   u'data': u'Slept for 0.1 seconds.',
                                   u'metadata': {'key': 'value'},
                                   u'logs': [],
                                   u'result_url': "http://0.0.0.0:9091/resul"},\
            job_status_data

        job = web.get_job(job_status_data['job_id'])
        assert not job['api_key'], job

        rv = app.get('/job/with_result')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        assert job_status_data == {u'status': u'complete',
                                   u'sent_data': {u'time': 0.1},
                                   u'job_id': u'with_result',
                                   u'job_type': u'example',
                                   u'error': None,
                                   u'data': u'Slept for 0.1 seconds.',
                                   u'metadata': {'key': 'value'},
                                   u'logs': [],
                                   u'result_url': "http://0.0.0.0:9091/"
                                                 "result"}, \
            job_status_data

        last_request = json.loads(requests.get('http://0.0.0.0:9091/'
                                               'last_request').content)
        last_request['data'].pop('requested_timestamp')
        last_request['data'].pop('finished_timestamp')
        last_request['data'].pop('job_key')

        assert last_request[u'headers'][u'Content-Type'] == u'application/json'
        assert_equal(last_request[u'data'], {
                    "status": "complete",
                    "sent_data": {"time": 0.1},
                    "job_id": "with_result",
                    "job_type": "example",
                    "result_url": "http://0.0.0.0:9091/"
                                  "result",
                    "error": None,
                    "data": "Slept for 0.1 seconds.",
                    "metadata": {'key': 'value'},
                    "logs": [],
                    })

    def test_missing_job_id(self):
        '''Trying to get a job ID that doesn't exist should return an HTTP 404.

        The response body should be a JSON object containing a not found error.

        '''
        app = test_client()
        rv = app.get('/job/not_there')
        assert rv.status_code == 404, rv.status
        error = json.loads(rv.data)
        assert error == {u'error': u'job_id not found'}

    def test_not_authorized_to_view_job(self):
        '''Getting a job that you're not authorized to view should 403.'''
        app = test_client()
        rv = app.post(
            '/job/one_job',
            data=json.dumps({"job_type": "echo",
                             "api_key": 42}),
            content_type='application/json')
        assert rv.status_code == 200, rv.status
        job_status_data = json.loads(rv.data)
        job_key = job_status_data['job_key']
        rv = app.get('/job/one_job')

        assert rv.status_code == 403, rv.status
        error = json.loads(rv.data)
        assert error == {u'error': u'not authorized'}

        headers = {'Authorization': job_key}
        rv = app.get('/job/one_job', headers=headers)
        assert rv.status_code == 200, rv.status

    def test_bad_metadata(self):
        '''Posting a job with non-JSON mmetadata should error.'''
        app = test_client()
        rv = app.post(
            '/job/with_bad_metadata',
            data=json.dumps({"job_type": "example",
                             "api_key": 42,
                             "data": {"time": 0.1},
                             "metadata": "meta",
                             "result_url": "http//0.0.0.0:9091/result"}),
            content_type='application/json')

        return_value = json.loads(rv.data)
        assert return_value == {u'error': u'metadata has to be a '
                                          'json object'}, return_value

    def test_bad_url(self):
        '''Posting a job with an invalid result_url should error.'''
        app = test_client()
        rv = app.post(
            '/job/with_bad_result',
            data=json.dumps({"job_type": "example",
                             "api_key": 42,
                             "data": {"time": 0.1},
                             "metadata": "meta",
                             "result_url": "ht//0.0.0.0:9091/resul"}),
            content_type='application/json')

        return_value = json.loads(rv.data)
        assert return_value == {u'error': u'result_url has to start '
                                          'with http'}, return_value

    def test_zz_misfire(self):
        '''Jobs should error if not completed within the misfire_grace_time.'''
        app = test_client()
        #has z because if this test failes will cause other tests to fail'''

        web.scheduler.misfire_grace_time = 0.000001
        rv = app.post(
            '/job/misfire',
            data=json.dumps({"job_type": "example",
                             "api_key": 42,
                             "data": {"time": 0.1},
                             "metadata": {"moon": "moon",
                                          "nested": {"nested": "nested"},
                                          "key": "value"},
                             }),
            content_type='application/json')

        time.sleep(0.5)

        login(app)
        rv = app.get('/job/misfire')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        assert_equal(job_status_data, {u'status': u'error',
                                   u'sent_data': {u'time': 0.1},
                                   u'job_id': u'misfire',
                                   u'job_type': u'example',
                                   u'result_url': None,
                                   u'error': u'Job delayed too long, '
                                             'service full',
                                   u'data': None,
                                   u'logs': [],
                                   u'metadata': {"key": "value",
                                                 "moon": "moon",
                                                 "nested": {"nested":
                                                            "nested"}}})
        web.scheduler.misfire_grace_time = 3600

    def test_synchronous_raw_post(self):
        '''Posting a raw synchronous job should get result in response body.

        User posts a "raw" synchronous job request, ckan-service-provider runs
        the job and returns an HTTP response with the job result as body.
        (A "raw" job is one whose result is a raw text value rather than JSON
        text.)

        '''
        app = test_client()
        rv = app.post('/job/echoraw',
                      data=json.dumps({"metadata": {"key": "value",
                                                    "moo": "moo"},
                                       "job_type": "echo_raw",
                                       "api_key": 42,
                                       "data": "ping"}),
                      content_type='application/json')
        assert rv.data == 'ginp'

    def test_synchronous_post(self):
        '''Posting a synchronous job should get a JSON response with result.

        User posts a synchronous job request, ckan-service-provider runs the
        job and returns an HTTP response with a JSON body containing the job
        result.

        '''
        app = test_client()
        rv = app.post('/job/echobasic',
                      data=json.dumps({"metadata": {"key": "value",
                                                    "moo": "moo",
                                                    "mimetype": "text/csv"},
                                       "job_type": "echo",
                                       "api_key": 42,
                                       "data": "ping"}),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        return_data.pop('requested_timestamp')
        return_data.pop('finished_timestamp')
        job_key = return_data.pop('job_key')

        job = web.get_job(return_data['job_id'])
        assert not job['api_key'], job

        assert_equal(return_data, {u'status': u'complete',
                               u'sent_data': u'ping',
                               u'job_id': u'echobasic',
                               u'job_type': u'echo',
                               u'result_url': None,
                               u'error': None,
                               u'data': u'>ping',
                               u'logs': [],
                               u'metadata': {"key": "value",
                                             "moo": "moo",
                                             "mimetype": "text/csv"}})

        login(app)
        rv = app.get('/job/echobasic')
        assert rv.status_code == 200, rv.status
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')

        assert_equal(return_data, job_status_data)

        headers = {'Authorization': job_key}
        rv = app.get('/job/echobasic/data', headers=headers)
        assert rv.status_code == 200, rv.status
        assert_equal(rv.data, u'>ping')
        assert 'text/csv' in rv.content_type, rv.content_type

        rv = app.post('/job/echobasic',
                      data=json.dumps({"job_type": "echo",
                                       "api_key": 42,
                                       "data": "ping"}),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        assert_equal(return_data, {u'error': u'job_id echobasic already exists'})

        rv = app.post('/job/echoknownbad',
                      data=json.dumps({"job_type": "echo",
                                       "api_key": 42,
                                       "data": ">ping"}),
                      content_type='application/json')
        assert rv.status_code == 200, rv.status
        return_data = json.loads(rv.data)
        return_data.pop('requested_timestamp')
        return_data.pop('finished_timestamp')
        return_data.pop('job_key')
        assert_equal(return_data, {u'status': u'error',
                               u'sent_data': u'>ping',
                               u'job_id': u'echoknownbad',
                               u'job_type': u'echo',
                               u'result_url': None,
                               u'error': u'Do not start message with >',
                               u'data': None,
                               u'logs': [],
                               u'metadata': {}})

        rv = app.post('/job/echounknownbad',
                      data=json.dumps({"job_type": "echo",
                                       "api_key": 42,
                                       "data": 1}),
                      content_type='application/json')
        return_data = json.loads(rv.data)
        assert 'AttributeError' in return_data['error']

        rv = app.post('/job/echobad_url',
                      data=json.dumps({"job_type": "echo",
                                       "api_key": 42,
                                       "data": "moo",
                                       "result_url": "http://bad_url"}),
                      content_type='application/json')
        return_data = json.loads(rv.data)
        return_data.pop('requested_timestamp')
        return_data.pop('finished_timestamp')
        return_data.pop('job_key')
        assert_equal(return_data, {u'status': u'complete',
                               u'sent_data': u'moo',
                               u'job_id': u'echobad_url',
                               u'job_type': u'echo',
                               u'result_url': u'http://bad_url',
                               u'error': u'Process completed but unable to'
                                          ' post to result_url',
                               u'data': u'>moo',
                               u'logs': [],
                               u'metadata': {}})

    def test_logging(self):
        '''Getting /job/log should return logs from the job as JSON.

        Jobs can log messages using a standard logger with a StoringHandler
        attached, and users can retrieve the logged messages using the
        /job/log API.

        '''
        app = test_client()
        rv = app.post('/job/log',
                      data=json.dumps({"metadata": {},
                                       "job_type": "log",
                                       "api_key": 42,
                                       "data": "&ping"}),
                      content_type='application/json')

        time.sleep(0.2)

        login(app, username='testadmin', password='wrong')
        rv = app.get('/job/log')
        assert rv.status_code == 403, rv.status

        login(app)
        rv = app.get('/job/log')
        assert rv.status_code == 200, rv.status

        return_data = json.loads(rv.data)
        logs = return_data['logs']
        assert len(logs) == 1, logs
        log = logs[0]
        log.pop('timestamp')
        log.pop('lineno')
        assert_equal(log, {
            u'level': u'WARNING',
            u'module': u'test_web',
            u'funcName': u'log',
            u'message': u'Just a warning'})

    def test_delete_job(self):
        '''Trying to get the status of a deleted job should return 404.

        This also tests that trying to delete a job when you're not authorized
        returns 403.

        '''
        app = test_client()
        rv = app.post('/job/to_be_deleted',
                      data=json.dumps({"metadata": {"foo": "bar"},
                                       "job_type": "echo",
                                       "api_key": 42,
                                       "data": "&ping"}),
                      content_type='application/json')
        assert rv.status_code == 200, rv.status

        rv = app.delete('/job/to_be_deleted')
        assert rv.status_code == 403, rv.status

        login(app)
        rv = app.delete('/job/to_be_deleted')
        assert rv.status_code == 200, rv.status

        rv = app.delete('/job/to_be_deleted')
        assert rv.status_code == 404, rv.status

    def test_getting_job_data_for_missing_job(self):
        '''Getting the job data for a job that doesn't exist should 404.'''
        app = test_client()
        login(app)
        rv = app.get('/job/somefoo/data')
        assert rv.status_code == 404, rv.status

    def test_list(self):
        '''Tests for /job which should return a list of all the jobs.

        Tests the results from getting /job with various different limits and
        filters.

        '''
        app = test_client()

        db.add_pending_job(
            "job_01", str(uuid.uuid4()), "job_type", "result_url", "api_key",
            metadata={"key": "value"})
        db.mark_job_as_completed("job_01")
        db.add_pending_job(
            "job_02", str(uuid.uuid4()), "job_type", "result_url", "api_key",
            metadata={"key": "value", "moo": "moo"})
        db.mark_job_as_completed("job_02")
        db.add_pending_job(
            "job_03", str(uuid.uuid4()), "job_type", "result_url", "api_key",
            metadata={"key": "value", "moo": "moo"})
        db.mark_job_as_completed("job_03")
        db.add_pending_job(
            "job_04", str(uuid.uuid4()), "job_type", "result_url", "api_key",
            metadata={"key": "value"})
        db.mark_job_as_completed("job_04")
        db.add_pending_job(
            "job_05", str(uuid.uuid4()), "job_type", "result_url", "api_key")
        db.mark_job_as_completed("job_05")
        db.add_pending_job(
            "job_06", str(uuid.uuid4()), "job_type", "result_url", "api_key")
        db.mark_job_as_completed("job_06")
        db.add_pending_job(
            "job_07", str(uuid.uuid4()), "job_type", "result_url", "api_key")
        db.mark_job_as_completed("job_07")
        db.add_pending_job(
            "job_08", str(uuid.uuid4()), "job_type", "result_url", "api_key")
        db.mark_job_as_completed("job_08")
        db.add_pending_job(
            "job_09", str(uuid.uuid4()), "job_type", "result_url", "api_key")
        db.add_pending_job(
            "job_10", str(uuid.uuid4()), "job_type", "result_url", "api_key")
        db.add_pending_job(
            "job_11", str(uuid.uuid4()), "job_type", "result_url", "api_key")
        db.add_pending_job(
            "job_12", str(uuid.uuid4()), "job_type", "result_url", "api_key")
        db.add_pending_job(
            "job_13", str(uuid.uuid4()), "job_type", "result_url", "api_key")

        rv = app.get('/job')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 13, return_data['list']

        rv = app.get('/job?_limit=1')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 1, return_data['list']

        rv = app.get('/job?_status=complete')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 8, return_data['list']

        rv = app.get('/job?key=value')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 4, return_data['list']

        rv = app.get('/job?key=value&moo=moo')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 2, return_data['list']

        rv = app.get('/job?key=value&moo=moo&moon=moon')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 0, return_data['list']

        rv = app.get('/job?key=value&moon=moon')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 0, return_data['list']


    def number_of_jobs(self, app):
        return len(json.loads(app.get("/job").data)["list"])


    def test_clear_all(self):
        '''Making a DELETE request to /job, which should delete all jobs.

        This also tests the 403 response when you're not authorized to delete,
        and tests the ?days argument.

        '''
        app = test_client()

        # Add some jobs, all completed and therefore eligible for deletion.
        db.add_pending_job(
            "job_01", str(uuid.uuid4()), "job_type", "result_url", "api_key",
            metadata={"key": "value"})
        db.mark_job_as_completed("job_01")
        db.add_pending_job(
            "job_02", str(uuid.uuid4()), "job_type", "result_url", "api_key",
            metadata={"key": "value", "moo": "moo"})
        db.mark_job_as_completed("job_02")
        db.add_pending_job(
            "job_03", str(uuid.uuid4()), "job_type", "result_url", "api_key",
            metadata={"key": "value", "moo": "moo"})
        db.mark_job_as_completed("job_03")

        original_number_of_jobs = self.number_of_jobs(app)

        # This should not delete any jobs because not authorized.
        rv = app.delete('/job')
        assert rv.status_code == 403, rv.status
        assert self.number_of_jobs(app) == original_number_of_jobs

        login(app)

        # This should not delete any jobs because the jobs aren't old enough.
        rv = app.delete('/job')
        assert rv.status_code == 200, rv.status
        assert self.number_of_jobs(app) == original_number_of_jobs

        # This should delete all the jobs.
        rv = app.delete('/job?days=0')
        assert rv.status_code == 200, rv.status
        assert self.number_of_jobs(app) == 0
