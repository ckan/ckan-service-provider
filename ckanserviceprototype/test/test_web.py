import os
import subprocess
import json
import time

import requests
import ckanserviceprototype.web as web
os.environ['JOB_CONFIG'] = 'test.ini'

try:
    os.remove('/tmp/datastorer_tasks.db')
except OSError:
    pass
web.configure()

app = web.app.test_client()
class TestWeb():

    @classmethod
    def setup_class(cls):
        fake_ckan_path = os.path.join(os.path.dirname(__file__), "fake_ckan.py")
        cls.fake_ckan = subprocess.Popen(['python', fake_ckan_path])
        #make sure service is running
        for i in range(0,50):
            time.sleep(0.1)
            response1 = requests.get('http://0.0.0.0:50001')
            if not response1:
                continue
            return
        cls.teardown_class()
        raise Exception('services did not start!')

    @classmethod
    def teardown_class(cls):
        cls.fake_ckan.kill()
        
    def test_status(self):
        rv = app.get('/status')
        assert json.loads(rv.data) == dict(version=0.1,
                                           job_types=['example'],
                                           name='datastorer')

    def test_bad_post(self):
        
        rv = app.post('/job', data='{"ffsfsafsa":"moo"}')
        assert json.loads(rv.data) == {u'error': u'Not recognised as json, make sure content type is application/json'}, json.loads(rv.data) 

        rv = app.post('/job',
                      data='{"ffsfsafsa":moo}',
                      content_type='application/json')
        assert json.loads(rv.data) == {u'error': u'Malformed json'}, json.loads(rv.data)

        rv = app.post('/job',
                      data=json.dumps({"data": {"time": 5}}),
                      content_type='application/json')
        assert json.loads(rv.data) == {u'error': u'Please specify a job type'}, json.loads(rv.data)

        rv = app.post('/job',
                      data=json.dumps({"job_type": "moo", "data": {"time": 5}}),
                      content_type='application/json')
        assert json.loads(rv.data) == {u'error': u'Job type moo not availiable. Availible job types are example'}, json.loads(rv.data)

    def test_asyncronous_post(self):

        # good job
        rv = app.post('/job',
                      data=json.dumps({"job_type": "example", "data": {"time": 0.1}}),
                      content_type='application/json')

        assert 'job_id' in json.loads(rv.data) 


        # good job with name
        rv = app.post('/job/moo',
                      data=json.dumps({"job_type": "example", "data": {"time": 0.1}}),
                      content_type='application/json')

        assert json.loads(rv.data) == {"job_id": "moo"}, json.loads(rv.data)

        rv = app.get('/job/moo')

        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        assert job_status_data == { u'status': u'pending', u'sent_data': {"time": 0.1}, u'job_id': u'moo', u'finished_timestamp': None, u'job_type': u'example', u'error': None, u'data': None, u'metadata': None, 'result_url': None}, job_status_data


        # bad job with same name
        rv = app.post('/job/moo',
                      data=json.dumps({"job_type": "example", "data": {"time": 0.1}}),
                      content_type='application/json')

        assert json.loads(rv.data) == {u'error': u'job_id moo already exists'}, json.loads(rv.data)

        # bad job missing the time param
        rv = app.post('/job/missing_time',
                      data=json.dumps({"job_type": "example", "data": {}}),
                      content_type='application/json')

        assert json.loads(rv.data) == {"job_id": "missing_time"}, json.loads(rv.data)

        # bad job missing the time param
        rv = app.post('/job/exception',
                      data=json.dumps({"job_type": "example", "data": {"time": "not_a_time"}}),
                      content_type='application/json')

        assert json.loads(rv.data) == {"job_id": "exception"}, json.loads(rv.data)


        # after sleeping for 0.2 seconds, jobs should now be complete
        time.sleep(0.2)

        rv = app.get('/job/moo')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        assert job_status_data == {u'status': u'complete', u'sent_data': {"time": 0.1}, u'job_id': u'moo', u'job_type': u'example', u'error': None, u'data': u'Slept for 0.1 seconds.', u'metadata': None, 'result_url': None}, job_status_data

        # job with known error
        rv = app.get('/job/missing_time')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        assert job_status_data == {u'status': u'error', u'sent_data': {}, u'job_id': u'missing_time', u'job_type': u'example', u'error': u'time not in input', u'data': None, u'metadata': None, 'result_url': None}, job_status_data

        # job with unexpected error
        rv = app.get('/job/exception')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        error = job_status_data.pop('error')
        assert job_status_data == {u'status': u'error', u'sent_data': {"time": "not_a_time"}, u'job_id': u'exception', u'job_type': u'example', u'data': None, u'metadata': None, 'result_url': None}, job_status_data
        assert 'TypeError' in error

    def test_asyncronous_post_with_return_url(self):

        rv = app.post(
            '/job/with_result',
            data=json.dumps({"job_type": "example",
                             "data": {"time": 0.1},
                             "metadata": "meta",
                             "result_url": "http://0.0.0.0:50001/result",
                             "api_key": "header:key"}),
            content_type='application/json')

        # bad result
        rv = app.post(
            '/job/with_bad_result',
            data=json.dumps({"job_type": "example",
                             "data": {"time": 0.1},
                             "metadata": "meta",
                             "result_url": "http://0.0.0.0:50001/resul",
                             "api_key": "key"}),
            content_type='application/json')

        time.sleep(0.5)

        rv = app.get('/job/with_bad_result')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        assert job_status_data == {u'status': u'complete', u'sent_data': {u'time': 0.1}, u'job_id': u'with_bad_result', u'job_type': u'example', u'error': u'Process completed but unable to post to result_url', u'data': u'Slept for 0.1 seconds.', u'metadata': u'meta', 'result_url': "http://0.0.0.0:50001/resul"}, job_status_data


        rv = app.get('/job/with_result')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        assert job_status_data == {u'status': u'complete', u'sent_data': {u'time': 0.1}, u'job_id': u'with_result', u'job_type': u'example', u'error': None, u'data': u'Slept for 0.1 seconds.', u'metadata': u'meta', 'result_url': "http://0.0.0.0:50001/result"}, job_status_data

        last_request = json.loads(requests.get('http://0.0.0.0:50001/last_request').content)
        last_request['data'].pop('requested_timestamp')
        last_request['data'].pop('finished_timestamp')

        assert last_request == {
                  "headers": {
                    "Content-Length": "317",
                    "Accept-Encoding": "gzip",
                    "Connection": "close",
                    "User-Agent": "python-requests.org",
                    "Host": "0.0.0.0:50001",
                    "Content-Type": "application/json",
                    "Header": "key"
                  },
                  "data": {
                    "status": "complete",
                    "sent_data": {
                      "time": 0.1
                    },
                    "job_id": "with_result",
                    "job_type": "example",
                    "result_url": "http://0.0.0.0:50001/result",
                    "error": None,
                    "data": "Slept for 0.1 seconds.",
                    "metadata": "meta"
                  }
                }, last_request

    def test_missing_job_id(self):
        rv = app.get('/job/not_there')
        assert rv.status_code == 404, rv.status
        error = json.loads(rv.data)
        assert error == {u'error': u'job_id not found'}
