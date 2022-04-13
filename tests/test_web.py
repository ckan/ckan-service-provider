import os
import sys
import json
import time
import logging
import uuid
import threading

import httpretty
import pytest

import ckanserviceprovider.web as web
import ckanserviceprovider.job as job
import ckanserviceprovider.util as util
import ckanserviceprovider.db as db


# The callback URL that ckanserviceprovider will post to when the
# asynchronous background job finishes. We will mock this.
RESULT_URL = "http://0.0.0.0/ckanserviceprovider/result_url"


def configure():
    """Configure the Flask app.

    This has to be called just once per test run (not e.g. once for each test).

    """
    os.environ["JOB_CONFIG"] = os.path.join(
        os.path.dirname(__file__), "settings_test.py"
    )
    web.init()


configure()


def reset_db():
    """Reset the database and scheduler.

    Should be called after each test.

    """
    web.scheduler.shutdown(wait=True)
    db.drop_all()
    db.init(web.app.config.get("SQLALCHEMY_DATABASE_URI"))
    web.init_scheduler(web.app.config.get("SQLALCHEMY_DATABASE_URI"))


def test_client():
    """Return a test client for the ckanserviceprovider web app."""
    return web.app.test_client()


def login(client, username="testadmin", password="testpass"):
    return client.post(
        "/login", data=dict(username=username, password=password), follow_redirects=True
    )


def _make_request_callback_function(event):
    """Return an httpretty request callback function that sets the given event.

    This is a helper function for mock_result_url() below.

    """

    def request_callback(request, uri, headers):
        event.set()
        return (200, headers, "")

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
    httpretty.register_uri(httpretty.POST, result_url, body=request_callback)
    return event


def _mock_test_callback_url(client):
    """Mock the tests callback URL.

    _TEST_CALLBACK_URL is a special URL that CKAN service provider calls after
    it has completely finished with an asynchronous job. Waiting for this URL
    to be called enables tests to assert things that don't happen until after
    the normal client callback URL has been called, without any race conditions
    in the tests.

    Returns a threading.Event object that you can use to wait for the mock URL
    to be called by doing: event.wait().

    """
    test_callback_url = client.application.config.get("_TEST_CALLBACK_URL")
    event = threading.Event()
    request_callback = _make_request_callback_function(event)
    httpretty.register_uri(httpretty.GET, test_callback_url, body=request_callback)
    return event


def number_of_jobs(client):
    """Return the number of jobs that the app has in its database.

    :param client: a test client of the ckanserviceprovider flask app

    """
    return len(json.loads(client.get("/job").data)["list"])


@job.synchronous
def echo(task_id, input_):
    if input_["data"].startswith(">"):
        raise util.JobError("Do not start message with >")
    if input_["data"].startswith("#"):
        raise Exception("Something went totally wrong")
    return ">" + input_["data"]


@job.synchronous
def echo_raw(task_id, input_):
    if input_["data"].startswith(">"):
        raise util.JobError("Do not start message with >")

    def raw():
        for x in sorted(input_["data"]):
            yield x

    return raw


@job.asynchronous
def example(task_id, input_):
    if "time" not in input_["data"]:
        raise util.JobError("time not in input")

    time.sleep(input_["data"]["time"])
    return "Slept for " + str(input_["data"]["time"]) + " seconds."


@job.asynchronous
def failing(task_id, input_):
    time.sleep(0.1)
    raise util.JobError("failed")


@job.asynchronous
def log(task_id, input_):
    handler = util.StoringHandler(task_id, input_)
    logger = logging.Logger(task_id)
    logger.addHandler(handler)

    logger.warning("Just a warning")


class TestWeb(object):
    def teardown(self):
        reset_db()

    def test_get_job_id_with_limit(self):
        """Limiting logs works fine"""
        client = test_client()
        client.post(
            "/job/12345",
            data=json.dumps(
                {"job_type": "example", "api_key": 42, "data": {"time": 0.1}}
            ),
            content_type="application/json",
        )
        db.add_logs(job_id="12345", message="message1")
        db.add_logs(job_id="12345", message="message2")
        db.add_logs(job_id="12345", message="message3")

        # Make sure it works without limit
        response = client.get(
            "/job/12345", headers={"Authorization": "please_replace_me"}
        )
        return_data = json.loads(response.data)
        assert len(return_data["logs"]) == 3

        # Now test with limit
        response = client.get(
            "/job/12345?limit=2", headers={"Authorization": "please_replace_me"}
        )
        return_data = json.loads(response.data)
        assert len(return_data["logs"]) == 2

    def test_status(self):
        """/status should return JSON with the app version, job types, etc."""
        client = test_client()
        response = client.get("/status")
        status_data = json.loads(response.data)
        status_data.pop("stats")
        assert status_data == dict(
            version=0.1,
            job_types=sorted(["failing", "example", "log", "echo_raw", "echo"]),
            name="testing",
        )

    def test_content_type(self):
        """Pages should have content_type "application/json"."""
        client = test_client()
        for page in ["/job", "/status", "/job/foo"]:
            response = client.get(page)
            assert response.content_type == "application/json"

    def test_bad_post(self):
        """Invalid posts to /job should receive error messages in JSON."""
        client = test_client()
        response = client.post("/job", data='{"ffsfsafsa":"moo"}')
        try:
            assert json.loads(response.data) == {
                "error": "Not recognised as json, make sure content type is "
                "application/json"
            }
        except AssertionError:
            assert json.loads(response.data) == {"error": "Malformed json"}

        response = client.post(
            "/job", data='{"ffsfsafsa":moo}', content_type="application/json"
        )
        assert json.loads(response.data) == {"error": "Malformed json"}

        response = client.post(
            "/job",
            data=json.dumps({"api_key": 42, "data": {"time": 5}}),
            content_type="application/json",
        )
        assert json.loads(response.data) == {"error": "Please specify a job type"}

        response = client.post(
            "/job",
            data=json.dumps({"job_type": "moo", "api_key": 42, "data": {"time": 5}}),
            content_type="application/json",
        )
        assert json.loads(response.data) == {
            "error": "Job type moo not available. Available job types are "
            "echo, echo_raw, example, failing, log"
        }

        response = client.post(
            "/job",
            data=json.dumps({"job_type": "example", "data": {"time": 5}}),
            content_type="application/json",
        )
        assert json.loads(response.data) == {"error": "Please provide your API key."}

        response = client.post(
            "/job",
            data=json.dumps(
                {"job_type": "example", "api_key": 42, "data": {"time": 5}, "foo": 42}
            ),
            content_type="application/json",
        )
        assert json.loads(response.data) == {
            "error": "Too many arguments. Extra keys are foo"
        }

    def test_asynchronous_post_with_good_job(self):
        """A valid post to /job should get back a JSON object with a job ID."""
        client = test_client()
        response = client.post(
            "/job",
            data=json.dumps(
                {"job_type": "example", "api_key": 42, "data": {"time": 0.1}}
            ),
            content_type="application/json",
        )

        return_data = json.loads(response.data)
        assert "job_id" in return_data

    @httpretty.activate
    def test_callback_url_is_called_with_api_key(self):
        """It should use the API key when posting to the callback URL."""
        API_KEY = "42"
        client = test_client()
        event = threading.Event()

        def callback(request, uri, headers):
            assert request.headers.get("Authorization") == API_KEY, (
                "ckanserviceprovider should put the API key in the "
                "Authorization header when calling the callback URL"
            )
            event.set()
            return (200, headers, "")

        httpretty.register_uri(httpretty.POST, RESULT_URL, body=callback)

        client.post(
            "/job",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": API_KEY,
                    "data": {"time": 0.1},
                    "result_url": RESULT_URL,
                }
            ),
            content_type="application/json",
        )

        timeout = 10.0
        assert event.wait(
            timeout
        ), "result_url was not called within {timeout} seconds".format(timeout=timeout)

    @httpretty.activate
    def test_get_job_does_not_return_api_key(self):
        """The dict that get_job() returns should not contain the API key."""
        client = test_client()
        mock_result_url(RESULT_URL)
        event = _mock_test_callback_url(client)

        response = client.post(
            "/job",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": 42,
                    "data": {"time": 0.1},
                    "result_url": RESULT_URL,
                }
            ),
            content_type="application/json",
        )
        return_data = json.loads(response.data)

        timeout = 10.0
        assert event.wait(
            timeout
        ), "_TEST_CALLBACK_URL was not called within {timeout} " "seconds".format(
            timeout=timeout
        )

        job_ = db.get_job(return_data["job_id"])
        assert not job_["api_key"], job_

    def test_post_job_with_custom_id(self):
        """Posting a job with a custom ID should return the ID in the JSON."""
        client = test_client()

        response = client.post(
            "/job/moo",
            data=json.dumps(
                {"job_type": "example", "api_key": 42, "data": {"time": 0.1}}
            ),
            content_type="application/json",
        )

        assert json.loads(response.data)["job_id"] == "moo", json.loads(response.data)

    # FIXME: I think there's actually a race condition here - if the
    # asynchronous background job (running in another thread) finishes before
    # we get to the assert it'l fail.
    def test_get_job_while_pending(self):
        """Create a job with a custom ID and get the job while still pending.

        This tests the value of the job's metadata while the job is still in a
        pending status.

        """
        client = test_client()
        client.post(
            "/job/moo",
            data=json.dumps(
                {"job_type": "example", "api_key": 42, "data": {"time": 1}}
            ),
            content_type="application/json",
        )

        login(client)
        response = client.get("/job/moo")

        job_status_data = json.loads(response.data)
        job_status_data.pop("requested_timestamp")
        assert job_status_data == {
            "status": "pending",
            "sent_data": {"time": 1},
            "job_id": "moo",
            "finished_timestamp": None,
            "job_type": "example",
            "error": None,
            "data": None,
            "metadata": {},
            "logs": [],
            "result_url": None,
        }, job_status_data

    @httpretty.activate
    def test_get_job_when_completed(self):
        """Get a job with a custom ID after it has completed.

        Tests the value of the job's metadata after the job has completed.

        """
        client = test_client()
        event = mock_result_url(RESULT_URL)
        client.post(
            "/job/moo",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": 42,
                    "data": {"time": 0.1},
                    "result_url": RESULT_URL,
                }
            ),
            content_type="application/json",
        )

        timeout = 10.0
        assert event.wait(
            timeout
        ), "result_url was not called within {timeout} seconds".format(timeout=timeout)

        login(client)

        response = client.get("/job/moo")

        job_status_data = json.loads(response.data)
        job_status_data.pop("requested_timestamp")
        job_status_data.pop("finished_timestamp")

        assert job_status_data == {
            "status": "complete",
            "sent_data": {"time": 0.1},
            "job_id": "moo",
            "job_type": "example",
            "error": None,
            "data": "Slept for 0.1 seconds.",
            "metadata": {},
            "logs": [],
            "result_url": RESULT_URL,
        }, job_status_data

    def test_post_job_with_duplicate_custom_id(self):
        """Posting a job with a duplicate ID should error."""
        client = test_client()
        client.post(
            "/job/moo",
            data=json.dumps(
                {"job_type": "example", "api_key": 42, "data": {"time": 0.1}}
            ),
            content_type="application/json",
        )

        response = client.post(
            "/job/moo",
            data=json.dumps(
                {"job_type": "example", "api_key": 42, "data": {"time": 0.1}}
            ),
            content_type="application/json",
        )

        assert json.loads(response.data) == {
            "error": "job_id moo already " "exists"
        }, json.loads(response.data)

    def test_post_with_job_error(self):
        """If a job raises JobError the response should still contain job_id.

        If a job with a custom ID raises JobError then the "job_id" field in
        ckanserviceprovider's HTTP response should still contain the job's
        custom ID.

        """
        # The 'example' job type (defined above) will raise JobError for this
        # data because the data has no "time" key.
        client = test_client()
        response = client.post(
            "/job/missing_time",
            data=json.dumps({"job_type": "example", "api_key": 42, "data": {}}),
            content_type="application/json",
        )

        assert json.loads(response.data)["job_id"] == "missing_time", json.loads(
            response.data
        )

    def test_post_with_job_exception(self):
        """If a job raises an exception the HTTP response should have an error.

        If a job raises an arbitrary exception (e.g. because of a mistake in
        the job code) the response to the job post HTTP request should have
        "exception" instead of the job ID.

        """
        client = test_client()
        # The 'example' job type (defined above) will crash on this invalid
        # time value.
        response = client.post(
            "/job/exception",
            data=json.dumps(
                {"job_type": "example", "api_key": 42, "data": {"time": "not_a_time"}}
            ),
            content_type="application/json",
        )

        assert json.loads(response.data)["job_id"] == "exception", json.loads(
            response.data
        )

    @httpretty.activate
    def test_get_job_with_known_error(self):
        """Test getting a job that failed with a JobError.

        Among other things, we expect the job dict to have an "error" key with
        the error string from the job function as its value.

        """
        client = test_client()
        mock_result_url(RESULT_URL)
        event = _mock_test_callback_url(client)

        response = client.post(
            "/job/missing_time",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": 42,
                    "data": {},
                    "result_url": RESULT_URL,
                }
            ),
            content_type="application/json",
        )

        login(client)

        timeout = 10.0
        assert event.wait(
            timeout
        ), "_TEST_CALLBACK_URL was not called within {timeout} " "seconds".format(
            timeout=timeout
        )

        response = client.get("/job/missing_time")

        job_status_data = json.loads(response.data)
        job_status_data.pop("requested_timestamp")
        job_status_data.pop("finished_timestamp")

        assert job_status_data == {
            "status": "error",
            "sent_data": {},
            "job_id": "missing_time",
            "job_type": "example",
            "error": {"message": "time not in input"},
            "data": None,
            "metadata": {},
            "logs": [],
            "result_url": RESULT_URL,
        }, job_status_data

        # get_job() shouldn't return the API key, either.
        job_ = db.get_job(job_status_data["job_id"])
        assert not job_["api_key"], job_

    @httpretty.activate
    def test_get_job_with_unknown_error(self):
        """Test getting a job that failed with a random exception.

        A random exception type caused by an error in the job function code,
        as opposed to a deliberately raised JobError.

        """
        client = test_client()
        mock_result_url(RESULT_URL)
        event = _mock_test_callback_url(client)

        response = client.post(
            "/job/exception",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": 42,
                    "data": {"time": "not_a_time"},
                    "result_url": RESULT_URL,
                }
            ),
            content_type="application/json",
        )

        login(client)

        timeout = 10.0
        assert event.wait(
            timeout
        ), "_TEST_CALLBACK_URL was not called within {timeout} " "seconds".format(
            timeout=timeout
        )

        response = client.get("/job/exception")

        job_status_data = json.loads(response.data)
        job_status_data.pop("requested_timestamp")
        job_status_data.pop("finished_timestamp")
        error = job_status_data.pop("error")

        assert job_status_data == {
            "status": "error",
            "sent_data": {"time": "not_a_time"},
            "job_id": "exception",
            "job_type": "example",
            "data": None,
            "metadata": {},
            "logs": [],
            "result_url": RESULT_URL,
        }, job_status_data
        assert "TypeError" in error["message"], error["message"]

        # get_job() shouldn't return the API key, either.
        job_ = db.get_job(job_status_data["job_id"])
        assert not job_["api_key"], job_

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
                    "content-type application/json"
                )

                # Check that the result URL was called with the right data.
                data = json.loads(request.body)
                data.pop("requested_timestamp")
                data.pop("finished_timestamp")
                data.pop("job_key")
                assert data == {
                    "status": "complete",
                    "sent_data": {"time": 0.1},
                    "job_id": "with_result",
                    "job_type": "example",
                    "result_url": RESULT_URL,
                    "error": None,
                    "data": "Slept for 0.1 seconds.",
                    "metadata": {"key": "value"},
                    "logs": [],
                }
            finally:
                event.set()
            return (200, headers, request.body)

        httpretty.register_uri(httpretty.POST, RESULT_URL, body=result_url)

        response = client.post(
            "/job/with_result",
            data=json.dumps(
                {
                    "job_type": "example",
                    "data": {"time": 0.1},
                    "metadata": {"key": "value"},
                    "result_url": RESULT_URL,
                    "api_key": "header:key",
                }
            ),
            content_type="application/json",
        )

        # Wait until ckanserviceprovider has posted the result of its
        # asynchronous background job to the mocked result URL.
        timeout = 10.0
        assert event.wait(
            timeout
        ), "result_url was not posted to within {timeout} seconds".format(
            timeout=timeout
        )

        login(client)

        response = client.get("/job/with_result")
        job_status_data = json.loads(response.data)
        job_status_data.pop("requested_timestamp")
        job_status_data.pop("finished_timestamp")
        assert job_status_data == {
            "status": "complete",
            "sent_data": {"time": 0.1},
            "job_id": "with_result",
            "job_type": "example",
            "error": None,
            "data": "Slept for 0.1 seconds.",
            "metadata": {"key": "value"},
            "logs": [],
            "result_url": RESULT_URL,
        }, job_status_data

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
        event = _mock_test_callback_url(client)

        httpretty.register_uri(httpretty.POST, RESULT_URL, status=404)

        response = client.post(
            "/job/with_bad_result",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": 42,
                    "data": {"time": 0.1},
                    "metadata": {"key": "value"},
                    "result_url": RESULT_URL,
                }
            ),
            content_type="application/json",
        )

        timeout = 10.0
        assert event.wait(
            timeout
        ), "_TEST_CALLBACK_URL was not called within {timeout} " "seconds".format(
            timeout=timeout
        )

        login(client)
        response = client.get("/job/with_bad_result")
        job_status_data = json.loads(response.data)
        job_status_data.pop("requested_timestamp")
        job_status_data.pop("finished_timestamp")
        assert job_status_data == {
            "status": "complete",
            "sent_data": {"time": 0.1},
            "job_id": "with_bad_result",
            "job_type": "example",
            "error": {
                "message": "Process completed but unable to post to " "result_url"
            },
            "data": "Slept for 0.1 seconds.",
            "metadata": {"key": "value"},
            "logs": [],
            "result_url": RESULT_URL,
        }, job_status_data

        job_ = db.get_job(job_status_data["job_id"])
        assert not job_["api_key"], job_

    def test_missing_job_id(self):
        """Trying to get a job ID that doesn't exist should return an HTTP 404.

        The response body should be a JSON object containing a not found error.

        """
        client = test_client()
        response = client.get("/job/not_there")
        assert response.status_code == 404, response.status
        error = json.loads(response.data)
        assert error == {"error": "job_id not found"}

    def test_not_authorized_to_view_job(self):
        """Getting a job that you're not authorized to view should 403."""
        client = test_client()
        response = client.post(
            "/job/one_job",
            data=json.dumps({"job_type": "echo", "api_key": 42}),
            content_type="application/json",
        )

        assert response.status_code == 200, response.status
        job_status_data = json.loads(response.data)
        job_key = job_status_data["job_key"]
        response = client.get("/job/one_job")

        assert response.status_code == 403, response.status
        error = json.loads(response.data)
        assert error == {"error": "not authorized"}

        headers = {"Authorization": job_key}
        response = client.get("/job/one_job", headers=headers)
        assert response.status_code == 200, response.status

    def test_bad_metadata(self):
        """Posting a job with non-JSON metadata should error."""
        client = test_client()
        response = client.post(
            "/job/with_bad_metadata",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": 42,
                    "data": {"time": 0.1},
                    "metadata": "meta",
                }
            ),
            content_type="application/json",
        )

        return_value = json.loads(response.data)
        assert return_value == {
            "error": "metadata has to be a " "json object"
        }, return_value

    def test_bad_url(self):
        """Posting a job with an invalid result_url should error."""
        client = test_client()
        response = client.post(
            "/job/with_bad_result",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": 42,
                    "data": {"time": 0.1},
                    "metadata": "meta",
                    "result_url": "ht//0.0.0.0:9091/resul",
                }
            ),
            content_type="application/json",
        )

        return_value = json.loads(response.data)
        assert return_value == {
            "error": "result_url has to start " "with http"
        }, return_value

    @httpretty.activate
    def test_misfire(self):
        """Jobs should error if not completed within the misfire_grace_time."""
        client = test_client()
        event = mock_result_url(RESULT_URL)

        web.scheduler.misfire_grace_time = 0.000001
        response = client.post(
            "/job/misfire",
            data=json.dumps(
                {
                    "job_type": "example",
                    "api_key": 42,
                    "data": {"time": 0.1},
                    "metadata": {
                        "moon": "moon",
                        "nested": {"nested": "nested"},
                        "key": "value",
                    },
                    "result_url": RESULT_URL,
                }
            ),
            content_type="application/json",
        )

        timeout = 10.0
        assert event.wait(
            timeout
        ), "result_url was not called within {timeout} seconds".format(timeout=timeout)

        login(client)
        response = client.get("/job/misfire")
        job_status_data = json.loads(response.data)
        job_status_data.pop("requested_timestamp")
        job_status_data.pop("finished_timestamp")
        assert job_status_data == {
            "status": "error",
            "sent_data": {"time": 0.1},
            "job_id": "misfire",
            "job_type": "example",
            "error": {"message": "Job delayed too long, service full"},
            "data": None,
            "logs": [],
            "metadata": {
                "key": "value",
                "moon": "moon",
                "nested": {"nested": "nested"},
            },
            "result_url": RESULT_URL,
        }

    def test_synchronous_raw_post(self):
        """Posting a raw synchronous job should get result in response body.

        User posts a "raw" synchronous job request, ckan-service-provider runs
        the job and returns an HTTP response with the job result as body.
        (A "raw" job is one whose result is a raw text value rather than JSON
        text.)

        """
        client = test_client()
        response = client.post(
            "/job/echoraw",
            data=json.dumps(
                {
                    "metadata": {"key": "value", "moo": "moo"},
                    "job_type": "echo_raw",
                    "api_key": 42,
                    "data": "ping",
                }
            ),
            content_type="application/json",
        )

        if sys.version_info[0] < 3:
            assert response.data == "ginp"
        else:
            assert response.data == b"ginp"

    def test_synchronous_post(self):
        """Posting a synchronous job should get a JSON response with result.

        User posts a synchronous job request, ckan-service-provider runs the
        job and returns an HTTP response with a JSON body containing the job
        result.

        """
        client = test_client()
        response = client.post(
            "/job/echobasic",
            data=json.dumps(
                {
                    "metadata": {"key": "value", "moo": "moo", "mimetype": "text/csv"},
                    "job_type": "echo",
                    "api_key": 42,
                    "data": "ping",
                }
            ),
            content_type="application/json",
        )

        return_data = json.loads(response.data)
        return_data.pop("requested_timestamp")
        return_data.pop("finished_timestamp")
        job_key = return_data.pop("job_key")

        job_ = db.get_job(return_data["job_id"])
        assert not job_["api_key"], job_

        assert return_data == {
            "status": "complete",
            "sent_data": "ping",
            "job_id": "echobasic",
            "job_type": "echo",
            "result_url": None,
            "error": None,
            "data": ">ping",
            "logs": [],
            "metadata": {"key": "value", "moo": "moo", "mimetype": "text/csv"},
        }

        login(client)
        response = client.get("/job/echobasic")
        assert response.status_code == 200, response.status
        job_status_data = json.loads(response.data)
        job_status_data.pop("requested_timestamp")
        job_status_data.pop("finished_timestamp")

        assert return_data == job_status_data

        headers = {"Authorization": job_key}
        response = client.get("/job/echobasic/data", headers=headers)
        assert response.status_code == 200, response.status

        if sys.version_info[0] < 3:
            assert response.data == ">ping"
        else:
            assert response.data == b">ping"
        assert "text/csv" in response.content_type, response.content_type

        response = client.post(
            "/job/echobasic",
            data=json.dumps({"job_type": "echo", "api_key": 42, "data": "ping"}),
            content_type="application/json",
        )

        return_data = json.loads(response.data)
        assert return_data == {"error": "job_id echobasic already exists"}

        response = client.post(
            "/job/echoknownbad",
            data=json.dumps({"job_type": "echo", "api_key": 42, "data": ">ping"}),
            content_type="application/json",
        )
        assert response.status_code == 200, response.status
        return_data = json.loads(response.data)
        return_data.pop("requested_timestamp")
        return_data.pop("finished_timestamp")
        return_data.pop("job_key")
        assert return_data == {
            "status": "error",
            "sent_data": ">ping",
            "job_id": "echoknownbad",
            "job_type": "echo",
            "result_url": None,
            "error": {"message": "Do not start message with >"},
            "data": None,
            "logs": [],
            "metadata": {},
        }

        response = client.post(
            "/job/echounknownbad",
            data=json.dumps({"job_type": "echo", "api_key": 42, "data": 1}),
            content_type="application/json",
        )
        return_data = json.loads(response.data)
        assert "AttributeError" in return_data["error"]["message"]

        response = client.post(
            "/job/echobad_url",
            data=json.dumps(
                {
                    "job_type": "echo",
                    "api_key": 42,
                    "data": "moo",
                    "result_url": "http://bad_url",
                }
            ),
            content_type="application/json",
        )
        return_data = json.loads(response.data)
        return_data.pop("requested_timestamp")
        return_data.pop("finished_timestamp")
        return_data.pop("job_key")
        assert return_data == {
            "status": "complete",
            "sent_data": "moo",
            "job_id": "echobad_url",
            "job_type": "echo",
            "result_url": "http://bad_url",
            "error": {
                "message": "Process completed but unable to post to " "result_url"
            },
            "data": ">moo",
            "logs": [],
            "metadata": {},
        }

    @httpretty.activate
    def test_logging(self):
        """Getting /job/log should return logs from the job as JSON.

        Jobs can log messages using a standard logger with a StoringHandler
        attached, and users can retrieve the logged messages using the
        /job/log API.

        """
        client = test_client()
        event = mock_result_url(RESULT_URL)
        response = client.post(
            "/job/log",
            data=json.dumps(
                {
                    "metadata": {},
                    "job_type": "log",
                    "api_key": 42,
                    "data": "&ping",
                    "result_url": RESULT_URL,
                }
            ),
            content_type="application/json",
        )

        timeout = 10.0
        assert event.wait(
            timeout
        ), "result_url was not called within {timeout} seconds".format(timeout=timeout)

        login(client, username="testadmin", password="wrong")
        response = client.get("/job/log")
        assert response.status_code == 403, response.status

        login(client)
        response = client.get("/job/log")
        assert response.status_code == 200, response.status

        return_data = json.loads(response.data)
        logs = return_data["logs"]
        assert len(logs) == 1, logs
        log_ = logs[0]
        log_.pop("timestamp")
        log_.pop("lineno")
        assert log_ == {
            "level": "WARNING",
            "module": "test_web",
            "funcName": "log",
            "message": "Just a warning",
        }

    def test_delete_job(self):
        """Trying to get the status of a deleted job should return 404.

        This also tests that trying to delete a job when you're not authorized
        returns 403.

        """
        client = test_client()
        response = client.post(
            "/job/to_be_deleted",
            data=json.dumps(
                {
                    "metadata": {"foo": "bar"},
                    "job_type": "echo",
                    "api_key": 42,
                    "data": "&ping",
                }
            ),
            content_type="application/json",
        )
        assert response.status_code == 200, response.status

        response = client.delete("/job/to_be_deleted")
        assert response.status_code == 403, response.status

        login(client)
        response = client.delete("/job/to_be_deleted")
        assert response.status_code == 200, response.status

        response = client.delete("/job/to_be_deleted")
        assert response.status_code == 404, response.status

    def test_getting_job_data_for_missing_job(self):
        """Getting the job data for a job that doesn't exist should 404."""
        client = test_client()
        login(client)
        response = client.get("/job/somefoo/data")
        assert response.status_code == 404, response.status

    def test_list(self):
        """Tests for /job which should return a list of all the jobs.

        Tests the results from getting /job with various different limits and
        filters.

        """
        client = test_client()

        db.add_pending_job(
            "job_01",
            str(uuid.uuid4()),
            "job_type",
            "result_url",
            "api_key",
            metadata={"key": "value"},
        )
        db.mark_job_as_completed("job_01")
        db.add_pending_job(
            "job_02",
            str(uuid.uuid4()),
            "job_type",
            "result_url",
            "api_key",
            metadata={"key": "value", "moo": "moo"},
        )
        db.mark_job_as_completed("job_02")
        db.add_pending_job(
            "job_03",
            str(uuid.uuid4()),
            "job_type",
            "result_url",
            "api_key",
            metadata={"key": "value", "moo": "moo"},
        )
        db.mark_job_as_completed("job_03")
        db.add_pending_job(
            "job_04",
            str(uuid.uuid4()),
            "job_type",
            "result_url",
            "api_key",
            metadata={"key": "value"},
        )
        db.mark_job_as_completed("job_04")
        db.add_pending_job(
            "job_05", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )
        db.mark_job_as_completed("job_05")
        db.add_pending_job(
            "job_06", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )
        db.mark_job_as_completed("job_06")
        db.add_pending_job(
            "job_07", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )
        db.mark_job_as_completed("job_07")
        db.add_pending_job(
            "job_08", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )
        db.mark_job_as_completed("job_08")
        db.add_pending_job(
            "job_09", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )
        db.add_pending_job(
            "job_10", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )
        db.add_pending_job(
            "job_11", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )
        db.add_pending_job(
            "job_12", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )
        db.add_pending_job(
            "job_13", str(uuid.uuid4()), "job_type", "result_url", "api_key"
        )

        response = client.get("/job")
        return_data = json.loads(response.data)
        assert len(return_data["list"]) == 13, return_data["list"]

        response = client.get("/job?_limit=1")
        return_data = json.loads(response.data)
        assert len(return_data["list"]) == 1, return_data["list"]

        response = client.get("/job?_status=complete")
        return_data = json.loads(response.data)
        assert len(return_data["list"]) == 8, return_data["list"]

        response = client.get("/job?key=value")
        return_data = json.loads(response.data)
        assert len(return_data["list"]) == 4, return_data["list"]

        response = client.get("/job?key=value&moo=moo")
        return_data = json.loads(response.data)
        assert len(return_data["list"]) == 2, return_data["list"]

        response = client.get("/job?key=value&moo=moo&moon=moon")
        return_data = json.loads(response.data)
        assert len(return_data["list"]) == 0, return_data["list"]

        response = client.get("/job?key=value&moon=moon")
        return_data = json.loads(response.data)
        assert len(return_data["list"]) == 0, return_data["list"]

    def test_clear_all(self):
        """Making a DELETE request to /job should delete all jobs.

        This also tests the 403 response when you're not authorized to delete,
        and tests the ?days argument.

        """
        client = test_client()

        # Add some jobs, all completed and therefore eligible for deletion.
        db.add_pending_job(
            "job_01",
            str(uuid.uuid4()),
            "job_type",
            "result_url",
            "api_key",
            metadata={"key": "value"},
        )
        db.mark_job_as_completed("job_01")
        db.add_pending_job(
            "job_02",
            str(uuid.uuid4()),
            "job_type",
            "result_url",
            "api_key",
            metadata={"key": "value", "moo": "moo"},
        )
        db.mark_job_as_completed("job_02")
        db.add_pending_job(
            "job_03",
            str(uuid.uuid4()),
            "job_type",
            "result_url",
            "api_key",
            metadata={"key": "value", "moo": "moo"},
        )
        db.mark_job_as_completed("job_03")

        original_number_of_jobs = number_of_jobs(client)

        # This should not delete any jobs because not authorized.
        response = client.delete("/job")
        assert response.status_code == 403, response.status
        assert number_of_jobs(client) == original_number_of_jobs

        login(client)

        # This should not delete any jobs because the jobs aren't old enough.
        response = client.delete("/job")
        assert response.status_code == 200, response.status
        assert number_of_jobs(client) == original_number_of_jobs

        # This should delete all the jobs.
        response = client.delete("/job?days=0")
        assert response.status_code == 200, response.status
        assert number_of_jobs(client) == 0
