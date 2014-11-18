import os
import json
import time
import logging
import uuid
import threading

import httpretty
from nose.tools import assert_equal

import ckanserviceprovider.web as web
import ckanserviceprovider.job as job
import ckanserviceprovider.util as util
import ckanserviceprovider.db as db


# The callback URL that ckanserviceprovider will post to when the
# asynchronous background job finishes. We will mock this.
RESULT_URL = "http://demo.ckan.org/ckanserviceprovider/result_url"


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


def login(client, username='testadmin', password='testpass'):
    return client.post('/login', data=dict(
        username=username,
        password=password
    ), follow_redirects=True)


def _make_request_callback_function(event):
    """Return an httpretty request callback function that sets the given event.

    This is a helper function for mock_result_url() below.

    """
    def request_callback(request, uri, headers):
        event.set()
    return request_callback


def mock_result_url(result_url):
    """Mock the given CKAN Service Provider result URL.

    Returns a threading.Event object that you can use to wait for the mock URL
    to be called by doing: event.wait().

    The way it works is:

    * A test method calls this function to mock a result_url, receives a
      threading event object in return.

    * The test method posts to ckanserviceprovider passing the mocked
      result_url.

    * ckanserviceprovider kicks off an asynchronous background job.

    * The test method waits for ckanserviceprovider's asynchronous background
      job to finish by doing event.wait().

    * When the job finishes ckanserviceprovider posts to the result_url.

    * The post is intercepted and redirected to a function that sets the
      thread event.

    * event.wait() returns and the test method continues.

    """
    event = threading.Event()
    request_callback = _make_request_callback_function(event)
    httpretty.register_uri(
        httpretty.POST, result_url, body=request_callback)
    return event


@job.sync
def echo(task_id, input):
    if input['data'].startswith('>'):
        raise util.JobError('Do not start message with >')
    if input['data'].startswith('#'):
        raise Exception('Something went totally wrong')
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


class TestWeb(object):

    def teardown(self):
        reset_db()

    def test_status(self):
        '''/status should return JSON with the app version, job types, etc.'''
        client = test_client()
        rv = client.get('/status')
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
        client = test_client()
        # make sure that we get json
        for page in ['/job', '/status', '/job/foo']:
            rv = client.get(page)
            assert_equal(rv.content_type, 'application/json')

    def test_bad_post(self):
        '''Invalid posts to /job should receive error messages in JSON.'''
        client = test_client()
        rv = client.post('/job', data='{"ffsfsafsa":"moo"}')
        assert_equal(json.loads(rv.data), {u'error': u'Not recognised as json,'
                                           ' make sure content type'
                                           ' is application/json'})

        rv = client.post('/job',
                      data='{"ffsfsafsa":moo}',
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Malformed json'})

        rv = client.post('/job',
                      data=json.dumps({
                          "api_key": 42,
                          "data": {"time": 5}}),
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Please specify a job '
                                           'type'})

        rv = client.post('/job',
                      data=json.dumps({"job_type": "moo",
                                       "api_key": 42,
                                       "data": {"time": 5}}),
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Job type moo not available.'
                                           ' Available job types are '
                                           'failing, example, log, echo_raw, echo'})

        rv = client.post('/job',
                      data=json.dumps({"job_type": "example",
                                       "data": {"time": 5}}),
                      content_type='application/json')
        assert_equal(json.loads(rv.data), {u'error': u'Please provide your API key.'})

        rv = client.post('/job',
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
        client = test_client()
        # good job
        rv = client.post('/job',
                      data=json.dumps({"job_type": "example",
                                       "api_key": 42,
                                       "data": {"time": 0.1}}),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        assert 'job_id' in return_data

    @httpretty.activate
    def test_get_job_does_not_return_api_key(self):
        '''The dict that get_job() returns should not contain the API key.'''
        client = test_client()

        event = mock_result_url(RESULT_URL)

        response = client.post(
            '/job',
            data=json.dumps(
                {"job_type": "example",
                 "api_key": 42,
                 "data": {"time": 0.1},
                 "result_url": RESULT_URL}),
            content_type='application/json')
        return_data = json.loads(response.data)

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

        job = web.get_job(return_data['job_id'])
        assert not job['api_key'], job

    def test_post_job_with_custom_id(self):
        '''Posting a job with a custom ID should return the ID in the JSON.'''
        client = test_client()

        # good job with name
        rv = client.post('/job/moo',
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
        client = test_client()
        client.post(
            '/job/moo',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": 1}}),
            content_type='application/json')

        login(client)
        rv = client.get('/job/moo')

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

    @httpretty.activate
    def test_get_job_when_completed(self):
        '''Get a job with a custom ID after it has completed.

        Tests the value of the job's metadata after the job has completed.

        '''
        client = test_client()
        event = mock_result_url(RESULT_URL)
        client.post(
            '/job/moo',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": 0.1},
                "result_url": RESULT_URL}),
            content_type='application/json')

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

        login(client)

        rv = client.get('/job/moo')

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
                                   u'result_url': RESULT_URL}, job_status_data

    def test_post_job_with_duplicate_custom_id(self):
        '''Posting a job with a duplicate ID should error.'''
        client = test_client()
        client.post(
            '/job/moo',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": 0.1}}),
            content_type='application/json')

        rv = client.post(
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
        client = test_client()
        rv = client.post(
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
        client = test_client()
        # The 'example' job type (defined above) will crash on this invalid
        # time value.
        rv = client.post(
            '/job/exception',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": "not_a_time"}}),
            content_type='application/json')

        assert json.loads(rv.data)['job_id'] == "exception", \
            json.loads(rv.data)

    @httpretty.activate
    def test_get_job_with_known_error(self):
        '''Test getting a job that failed with a JobError.

        Among other things, we expect the job dict to have an "error" key with
        the error string from the job function as its value.

        '''
        client = test_client()
        event = mock_result_url(RESULT_URL)
        rv = client.post(
            '/job/missing_time',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {},
                "result_url": RESULT_URL}),
            content_type='application/json')

        login(client)

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

        rv = client.get('/job/missing_time')

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
                                   u'result_url': RESULT_URL}, job_status_data

        # get_job() shouldn't return the API key, either.
        job = web.get_job(job_status_data['job_id'])
        assert not job['api_key'], job

    @httpretty.activate
    def test_get_job_with_unknown_error(self):
        '''Test getting a job that failed with a random exception.

        A random exception type caused by an error in the job function code,
        as opposed to a deliberately raised JobError.

        '''
        client = test_client()
        event = mock_result_url(RESULT_URL)
        rv = client.post(
            '/job/exception',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": "not_a_time"},
                "result_url": RESULT_URL}),
            content_type='application/json')

        login(client)

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

        rv = client.get('/job/exception')

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
                                   u'result_url': RESULT_URL}, job_status_data
        assert 'TypeError' in error[-1], error

        # get_job() shouldn't return the API key, either.
        job = web.get_job(job_status_data['job_id'])
        assert not job['api_key'], job

    @httpretty.activate
    def test_asynchronous_post_with_result_url(self):
        """It should post job results to their result URLs.

        If a job has a result_url parameter then when the job finishes
        ckanserviceprovider should post the job's result to the result_url.


        """
        client = test_client()

        # A thread event that we'll set when the mocked result URL is posted to
        event = threading.Event()

        # Mock the result URL.
        def result_url(request, uri, headers):
            """Handle a request to the mocked result URL."""

            try:
                assert request.headers["content-type"] == "application/json", (
                    "ckanserviceprovider should post to result URLs with "
                    "content-type application/json")

                # Check that the result URL was called with the right data.
                data = json.loads(request.body)
                data.pop('requested_timestamp')
                data.pop('finished_timestamp')
                data.pop('job_key')
                assert_equal(
                    data,
                    {
                        "status": "complete",
                        "sent_data": {"time": 0.1},
                        "job_id": "with_result",
                        "job_type": "example",
                        "result_url": RESULT_URL,
                        "error": None,
                        "data": "Slept for 0.1 seconds.",
                        "metadata": {'key': 'value'},
                        "logs": [],
                    })
            finally:
                event.set()
        httpretty.register_uri(httpretty.POST, RESULT_URL, body=result_url)

        rv = client.post(
            '/job/with_result',
            data=json.dumps({"job_type": "example",
                             "api_key": 42,
                             "data": {"time": 0.1},
                             "metadata": {'key': 'value'},
                             "result_url": RESULT_URL,
                             "api_key": "header:key"}),
            content_type='application/json')

        # Wait until ckanserviceprovider has posted the result of its
        # asynchronous background job to the mocked result URL.
        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not posted to within {timeout} seconds".format(
                timeout=timeout))

        login(client)

        rv = client.get('/job/with_result')
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
                                   u'result_url': RESULT_URL}, job_status_data

    @httpretty.activate
    def test_asynchronous_post_with_bad_result_url(self):
        """It should store an error if given a bad result URL.

        If given an asynchronous job request with a bad result URL
        ckanserviceprovider should store a
        "Process completed but unable to post to result_url" error.

        This error overwrites any error that might have happened with the job
        itself!

        """
        client = test_client()
        test_callback_url = client.application.config.get("_TEST_CALLBACK_URL")
        event = threading.Event()
        def test_callback_was_called(request, uri, headers):
            event.set()
        httpretty.register_uri(httpretty.POST, RESULT_URL, status=404)
        httpretty.register_uri(
            httpretty.GET, test_callback_url, body=test_callback_was_called)

        rv = client.post(
            '/job/with_bad_result',
            data=json.dumps({
                "job_type": "example",
                "api_key": 42,
                "data": {"time": 0.1},
                "metadata": {'key': 'value'},
                "result_url": RESULT_URL}),
            content_type='application/json')

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

        login(client)
        rv = client.get('/job/with_bad_result')
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
                                   u'result_url': RESULT_URL}, job_status_data

        job = web.get_job(job_status_data['job_id'])
        assert not job['api_key'], job

    def test_missing_job_id(self):
        '''Trying to get a job ID that doesn't exist should return an HTTP 404.

        The response body should be a JSON object containing a not found error.

        '''
        client = test_client()
        rv = client.get('/job/not_there')
        assert rv.status_code == 404, rv.status
        error = json.loads(rv.data)
        assert error == {u'error': u'job_id not found'}

    def test_not_authorized_to_view_job(self):
        '''Getting a job that you're not authorized to view should 403.'''
        client = test_client()
        rv = client.post(
            '/job/one_job',
            data=json.dumps({"job_type": "echo",
                             "api_key": 42}),
            content_type='application/json')
        assert rv.status_code == 200, rv.status
        job_status_data = json.loads(rv.data)
        job_key = job_status_data['job_key']
        rv = client.get('/job/one_job')

        assert rv.status_code == 403, rv.status
        error = json.loads(rv.data)
        assert error == {u'error': u'not authorized'}

        headers = {'Authorization': job_key}
        rv = client.get('/job/one_job', headers=headers)
        assert rv.status_code == 200, rv.status

    def test_bad_metadata(self):
        '''Posting a job with non-JSON mmetadata should error.'''
        client = test_client()
        rv = client.post(
            '/job/with_bad_metadata',
            data=json.dumps({"job_type": "example",
                             "api_key": 42,
                             "data": {"time": 0.1},
                             "metadata": "meta"}),
            content_type='application/json')

        return_value = json.loads(rv.data)
        assert return_value == {u'error': u'metadata has to be a '
                                          'json object'}, return_value

    def test_bad_url(self):
        '''Posting a job with an invalid result_url should error.'''
        client = test_client()
        rv = client.post(
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

    @httpretty.activate
    def test_zz_misfire(self):
        '''Jobs should error if not completed within the misfire_grace_time.'''
        client = test_client()
        event = mock_result_url(RESULT_URL)
        #has z because if this test failes will cause other tests to fail'''

        web.scheduler.misfire_grace_time = 0.000001
        rv = client.post(
            '/job/misfire',
            data=json.dumps({"job_type": "example",
                             "api_key": 42,
                             "data": {"time": 0.1},
                             "metadata": {"moon": "moon",
                                          "nested": {"nested": "nested"},
                                          "key": "value"},
                             "result_url": RESULT_URL,
                             }),
            content_type='application/json')

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

        login(client)
        rv = client.get('/job/misfire')
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
                                                            "nested"}},
                                    "result_url": RESULT_URL})

    def test_synchronous_raw_post(self):
        '''Posting a raw synchronous job should get result in response body.

        User posts a "raw" synchronous job request, ckan-service-provider runs
        the job and returns an HTTP response with the job result as body.
        (A "raw" job is one whose result is a raw text value rather than JSON
        text.)

        '''
        client = test_client()
        rv = client.post('/job/echoraw',
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
        client = test_client()
        rv = client.post('/job/echobasic',
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

        login(client)
        rv = client.get('/job/echobasic')
        assert rv.status_code == 200, rv.status
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')

        assert_equal(return_data, job_status_data)

        headers = {'Authorization': job_key}
        rv = client.get('/job/echobasic/data', headers=headers)
        assert rv.status_code == 200, rv.status
        assert_equal(rv.data, u'>ping')
        assert 'text/csv' in rv.content_type, rv.content_type

        rv = client.post('/job/echobasic',
                      data=json.dumps({"job_type": "echo",
                                       "api_key": 42,
                                       "data": "ping"}),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        assert_equal(return_data, {u'error': u'job_id echobasic already exists'})

        rv = client.post('/job/echoknownbad',
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

        rv = client.post('/job/echounknownbad',
                      data=json.dumps({"job_type": "echo",
                                       "api_key": 42,
                                       "data": 1}),
                      content_type='application/json')
        return_data = json.loads(rv.data)
        assert 'AttributeError' in return_data['error']

        rv = client.post('/job/echobad_url',
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

    @httpretty.activate
    def test_logging(self):
        '''Getting /job/log should return logs from the job as JSON.

        Jobs can log messages using a standard logger with a StoringHandler
        attached, and users can retrieve the logged messages using the
        /job/log API.

        '''
        client = test_client()
        event = mock_result_url(RESULT_URL)
        rv = client.post('/job/log',
                      data=json.dumps({"metadata": {},
                                       "job_type": "log",
                                       "api_key": 42,
                                       "data": "&ping",
                                       "result_url": RESULT_URL}),
                      content_type='application/json')

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

        login(client, username='testadmin', password='wrong')
        rv = client.get('/job/log')
        assert rv.status_code == 403, rv.status

        login(client)
        rv = client.get('/job/log')
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
        client = test_client()
        rv = client.post('/job/to_be_deleted',
                      data=json.dumps({"metadata": {"foo": "bar"},
                                       "job_type": "echo",
                                       "api_key": 42,
                                       "data": "&ping"}),
                      content_type='application/json')
        assert rv.status_code == 200, rv.status

        rv = client.delete('/job/to_be_deleted')
        assert rv.status_code == 403, rv.status

        login(client)
        rv = client.delete('/job/to_be_deleted')
        assert rv.status_code == 200, rv.status

        rv = client.delete('/job/to_be_deleted')
        assert rv.status_code == 404, rv.status

    def test_getting_job_data_for_missing_job(self):
        '''Getting the job data for a job that doesn't exist should 404.'''
        client = test_client()
        login(client)
        rv = client.get('/job/somefoo/data')
        assert rv.status_code == 404, rv.status

    def test_list(self):
        '''Tests for /job which should return a list of all the jobs.

        Tests the results from getting /job with various different limits and
        filters.

        '''
        client = test_client()

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

        rv = client.get('/job')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 13, return_data['list']

        rv = client.get('/job?_limit=1')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 1, return_data['list']

        rv = client.get('/job?_status=complete')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 8, return_data['list']

        rv = client.get('/job?key=value')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 4, return_data['list']

        rv = client.get('/job?key=value&moo=moo')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 2, return_data['list']

        rv = client.get('/job?key=value&moo=moo&moon=moon')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 0, return_data['list']

        rv = client.get('/job?key=value&moon=moon')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 0, return_data['list']


    def number_of_jobs(self, client):
        return len(json.loads(client.get("/job").data)["list"])


    def test_clear_all(self):
        '''Making a DELETE request to /job, which should delete all jobs.

        This also tests the 403 response when you're not authorized to delete,
        and tests the ?days argument.

        '''
        client = test_client()

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

        original_number_of_jobs = self.number_of_jobs(client)

        # This should not delete any jobs because not authorized.
        rv = client.delete('/job')
        assert rv.status_code == 403, rv.status
        assert self.number_of_jobs(client) == original_number_of_jobs

        login(client)

        # This should not delete any jobs because the jobs aren't old enough.
        rv = client.delete('/job')
        assert rv.status_code == 200, rv.status
        assert self.number_of_jobs(client) == original_number_of_jobs

        # This should delete all the jobs.
        rv = client.delete('/job?days=0')
        assert rv.status_code == 200, rv.status
        assert self.number_of_jobs(client) == 0
