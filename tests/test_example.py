"""Functional tests for the example CKAN Service Provider app."""
import json
import os
import threading

import httpretty

import ckanserviceprovider.db as db
import ckanserviceprovider.web as web

import example.main


# TODO: Make this a shared helper function.
def _configure():
    """Configure the Flask app.

    This has to be called just once per test run (not e.g. once for each test
    method).

    """
    os.environ['JOB_CONFIG'] = os.path.join(
        os.path.dirname(__file__), '../example/example_settings.py')
    web.init()
_configure()


# TODO: Make this a shared helper function.
def _reset_db():
    """Reset the database and scheduler.

    Should be called after each test.

    """
    web.scheduler.shutdown(wait=True)
    db.drop_all()
    db.init(web.app.config.get('SQLALCHEMY_DATABASE_URI'))
    web.init_scheduler(web.app.config.get('SQLALCHEMY_DATABASE_URI'))


def _test_app():
    """Return a test client for the example web app."""
    return example.main.test_app()


class TestExample(object):

    """Functional tests for the example CKAN Service Provider app."""

    def teardown(self):
        """Reset the db after each test method."""
        _reset_db()

    def test_root(self):
        """Getting / should return some help text."""
        app = _test_app()

        response = app.get('/')

        response_json = json.loads(response.data)
        # FIXME: This should be some help text from the example app,
        # not a link to CKAN Service Provider's docs!
        assert response_json['help'] == (
            'Get help at http://ckan-service-provider.readthedocs.org/')

    def test_that_status_reports_correct_service_name(self):
        """Getting /status should report the correct service name."""
        app = _test_app()

        response = app.get('/status')

        response_json = json.loads(response.data)
        assert response_json['name'] == 'example-service'

    def test_that_status_reports_correct_version_string(self):
        """Getting /status should report the correct version string."""
        app = _test_app()

        response = app.get('/status')

        response_json = json.loads(response.data)
        # FIXME: 0.1 is the wrong result! This should be a version string not a
        # floating point number, and it should be the version of the example
        # app not of ckanserviceprovider!
        assert response_json['version'] == 0.1

    def test_that_status_reports_correct_job_types(self):
        """Getting /status should report the available job types correctly."""
        app = _test_app()

        response = app.get('/status')

        response_json = json.loads(response.data)
        # FIXME: It actually returns more job types thant it should because
        # ckanserviceprovider's global dicts of job types aren't reset between
        # tests. It should return the job types from the example app only.
        # For now get around this by just checking that the example job types
        # are there.
        for job_type in ['example_async_echo', 'example_async_ping',
                         'example_echo']:
            assert job_type in response_json['job_types']

    def test_synchronous_echo(self):
        """Test calling the synchronous echo job and getting the result."""
        app = _test_app()

        response = app.post(
            '/job',
            data=json.dumps(dict(
                job_type='example_echo',
                api_key='test-api-key',
                data='Helloooo',
                )),
            content_type="application/json")

        response_json = json.loads(response.data)
        assert response_json["status"] == "complete"
        assert response_json["data"] == ">Helloooo"

    def test_synchronous_echo_JobError(self):
        """Test the synchronous echo job when it raises JobError."""
        app = _test_app()

        response = app.post(
            '/job',
            data=json.dumps(dict(
                job_type='example_echo',
                api_key='test-api-key',
                data='>Helloooo',  # This input data will produce a JobError.
                )),
            content_type="application/json")

        response_json = json.loads(response.data)
        assert response_json["status"] == "error"
        assert response_json["data"] is None
        assert response_json["error"]["message"] == (
            "do not start message with >")

    def test_synchronous_echo_Exception(self):
        """Test the synchronous echo job when it raises Exception."""
        app = _test_app()

        response = app.post(
            '/job',
            data=json.dumps(dict(
                job_type='example_echo',
                api_key='test-api-key',
                data='#Helloooo',  # This input data will produce an Exception.
                )),
            content_type="application/json")

        response_json = json.loads(response.data)
        assert response_json["status"] == "error"
        assert response_json["data"] is None
        # FIXME: CKAN Service Provider returns the exception string from Python
        # as response_json["error"]["message"], including the source code file
        # path on the server, line number where the exception was raised, and
        # the contents of the source code line that raised it.
        # It should just return the string that was passed to
        # raise Exception("...") instead.
        assert response_json["error"]["message"]

    @httpretty.activate
    def test_async_echo(self):
        """Test calling the asynchronous async_echo job.

        And receiving the result asynchronously.

        """
        app = _test_app()
        event = threading.Event()
        RESULT_URL = "http://demo.ckan.org/result_url"

        def callback(request, uri, headers):
            """Receive the async response from the app to the callback URL."""
            result = json.loads(request.body)
            assert result['status'] == 'complete'
            assert result["sent_data"] == "Helloooooo"
            assert result["job_type"] == "example_async_echo"
            assert result["error"] is None
            assert result["data"] == ">Helloooooo"
            event.set()
            return (200, headers, "")
        httpretty.register_uri(httpretty.POST, RESULT_URL, body=callback)

        response = app.post(
            '/job',
            data=json.dumps(
                {"job_type": "example_async_echo",
                 "api_key": 42,
                 "data": "Helloooooo",
                 "result_url": RESULT_URL}),
            content_type='application/json')

        # Check the synchronous response that we get back from the app straight
        # away telling us that our asynchronous job has been accepted.
        response_json = json.loads(response.data)
        assert response_json["job_type"] == "example_async_echo"
        assert response_json["sent_data"] == "Helloooooo"

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

    @httpretty.activate
    def test_async_echo_with_JobError(self):
        """Test calling the async_echo job when it raises JobError."""
        app = _test_app()
        event = threading.Event()
        RESULT_URL = "http://demo.ckan.org/result_url"

        def callback(request, uri, headers):
            """Receive the async response from the app to the callback URL."""
            result = json.loads(request.body)
            assert result['status'] == 'error'
            assert result['sent_data'] == '>Helloooooo'
            assert result['job_type'] == 'example_async_echo'
            assert result['error']['message'] == 'do not start message with >'
            assert result['data'] is None
            event.set()
            return (200, headers, '')
        httpretty.register_uri(httpretty.POST, RESULT_URL, body=callback)

        response = app.post(
            '/job',
            data=json.dumps(
                {"job_type": "example_async_echo",
                 "api_key": 42,
                 "data": ">Helloooooo",  # This input will raise JobError.
                 "result_url": RESULT_URL}),
            content_type='application/json')

        # Check the synchronous response that we get back from the app straight
        # away telling us that our asynchronous job has been accepted.
        response_json = json.loads(response.data)
        assert response_json["job_type"] == "example_async_echo"
        assert response_json["sent_data"] == ">Helloooooo"

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

    @httpretty.activate
    def test_async_echo_with_Exception(self):
        """Test calling the async_echo job when it raises Exception."""
        app = _test_app()
        event = threading.Event()
        RESULT_URL = "http://demo.ckan.org/result_url"

        def callback(request, uri, headers):
            """Receive the async response from the app to the callback URL."""
            result = json.loads(request.body)
            assert result['status'] == 'error'
            assert result['sent_data'] == '#Helloooooo'
            assert result['job_type'] == 'example_async_echo'
            assert result['error']['message']
            assert result['data'] is None
            event.set()
            return (200, headers, '')
        httpretty.register_uri(httpretty.POST, RESULT_URL, body=callback)

        response = app.post(
            '/job',
            data=json.dumps(
                {"job_type": "example_async_echo",
                 "api_key": 42,
                 "data": "#Helloooooo",  # This input will raise JobError.
                 "result_url": RESULT_URL}),
            content_type='application/json')

        # Check the synchronous response that we get back from the app straight
        # away telling us that our asynchronous job has been accepted.
        response_json = json.loads(response.data)
        assert response_json["job_type"] == "example_async_echo"
        assert response_json["sent_data"] == "#Helloooooo"

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))

    @httpretty.activate
    def test_async_ping(self):
        """Test receiving the log messages from the async_ping job."""
        app = _test_app()
        event = threading.Event()
        RESULT_URL = "http://demo.ckan.org/result_url"

        def callback(request, uri, headers):
            """Receive the async response from the app to the callback URL."""
            result = json.loads(request.body)
            assert len(result['logs']) == 1
            assert result['logs'][0]['message'] == 'ping'
            event.set()
            return (200, headers, "")
        httpretty.register_uri(httpretty.POST, RESULT_URL, body=callback)

        response = app.post(
            '/job',
            data=json.dumps(
                {"job_type": "example_async_ping",
                 "api_key": 42,
                 "data": "Hello",
                 "result_url": RESULT_URL}),
            content_type='application/json')

        # Check the synchronous response that we get back from the app straight
        # away telling us that our asynchronous job has been accepted.
        response_json = json.loads(response.data)
        assert response_json["job_type"] == "example_async_ping"
        assert response_json["sent_data"] == "Hello"

        timeout = 10.0
        assert event.wait(timeout), (
            "result_url was not called within {timeout} seconds".format(
                timeout=timeout))
