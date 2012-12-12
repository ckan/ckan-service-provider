import os
import subprocess
import json
import time

import requests
import ckanserviceprovider.web as web
import ckanserviceprovider.job as job
import ckanserviceprovider.util as util
os.environ['JOB_CONFIG'] = os.path.join(os.path.dirname(__file__),
                                        'test.ini')

try:
    os.remove('/tmp/job_store.db')
except OSError:
    pass
web.configure()
app = web.app.test_client()


@job.sync
def echo(task_id, input):
    if input['data'].startswith('>'):
        raise util.JobError('do not start message with >')
    return '>' + input['data']


@job.sync
def echo_raw(task_id, input):
    if input['data'].startswith('>'):
        raise util.JobError('do not start message with >')

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

    def test_status(self):
        rv = app.get('/status')
        assert json.loads(rv.data) == dict(version=0.1,
                                           job_types=['example',
                                                      'echo_raw',
                                                      'echo'],
                                           name='testing'), rv.data

    def test_content_type(self):
        # make sure that we get json
        for page in ['/job', '/status', '/job/foo']:
            rv = app.get(page)
            assert rv.content_type == 'application/json', rv.content_type

    def test_bad_post(self):

        rv = app.post('/job', data='{"ffsfsafsa":"moo"}')
        assert json.loads(rv.data) == {u'error': u'Not recognised as json,'
                                                 ' make sure content type'
                                                 ' is application/json'}, \
            json.loads(rv.data)

        rv = app.post('/job',
                      data='{"ffsfsafsa":moo}',
                      content_type='application/json')
        assert json.loads(rv.data) == {u'error': u'Malformed json'}, \
            json.loads(rv.data)

        rv = app.post('/job',
                      data=json.dumps({"data": {"time": 5}}),
                      content_type='application/json')
        assert json.loads(rv.data) == {u'error': u'Please specify a job '
                                                 'type'}, json.loads(rv.data)

        rv = app.post('/job',
                      data=json.dumps({"job_type": "moo",
                                       "data": {"time": 5}}),
                      content_type='application/json')
        assert json.loads(rv.data) == {u'error': u'Job type moo not available.'
                                                 ' Available job types are '
                                                 'example, echo_raw, echo'}, \
            json.loads(rv.data)

    def test_asyncronous_post(self):

        # good job
        rv = app.post('/job',
                      data=json.dumps({"job_type": "example",
                                       "data": {"time": 0.1}}),
                      content_type='application/json')

        assert 'job_id' in json.loads(rv.data)

        # good job with name
        rv = app.post('/job/moo',
                      data=json.dumps({"job_type": "example",
                                       "data": {"time": 0.1}}),
                      content_type='application/json')

        assert json.loads(rv.data) == {"job_id": "moo"}, json.loads(rv.data)

        rv = app.get('/job/moo')

        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        assert job_status_data == {u'status': u'pending',
                                   u'sent_data': {"time": 0.1},
                                   u'job_id': u'moo',
                                   u'finished_timestamp': None,
                                   u'job_type': u'example',
                                   u'error': None,
                                   u'data': None,
                                   u'metadata': {},
                                   'result_url': None}, job_status_data

        # bad job with same name
        rv = app.post('/job/moo',
                      data=json.dumps({"job_type": "example",
                                       "data": {"time": 0.1}}),
                      content_type='application/json')

        assert json.loads(rv.data) == {u'error': u'job_id moo '
                                                 'already exists'}, \
            json.loads(rv.data)

        # bad job missing the time param
        rv = app.post('/job/missing_time',
                      data=json.dumps({"job_type": "example", "data": {}}),
                      content_type='application/json')

        assert json.loads(rv.data) == {"job_id": "missing_time"}, \
            json.loads(rv.data)

        # bad job missing the time param
        rv = app.post('/job/exception',
                      data=json.dumps({"job_type": "example",
                                       "data": {"time": "not_a_time"}}),
                      content_type='application/json')

        assert json.loads(rv.data) == {"job_id": "exception"}, \
            json.loads(rv.data)

        # after sleeping for 0.2 seconds, jobs should now be complete
        time.sleep(0.2)

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
                                   'result_url': None}, job_status_data

        # job with known error
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
                                   'result_url': None}, job_status_data

        # job with unexpected error
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
                                   'result_url': None}, job_status_data
        assert 'TypeError' in error

    def test_asyncronous_post_with_return_url(self):

        rv = app.post(
            '/job/with_result',
            data=json.dumps({"job_type": "example",
                             "data": {"time": 0.1},
                             "metadata": {'key': 'value'},
                             "result_url": "http://0.0.0.0:9091/result",
                             "api_key": "header:key"}),
            content_type='application/json')

        # bad result
        rv = app.post(
            '/job/with_bad_result',
            data=json.dumps({"job_type": "example",
                             "data": {"time": 0.1},
                             "metadata": {'key': 'value'},
                             "result_url": "http://0.0.0.0:9091/resul",
                             "api_key": "key"}),
            content_type='application/json')

        time.sleep(0.5)

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
                                   'result_url': "http://0.0.0.0:9091/resul"},\
            job_status_data

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
                                   'result_url': "http://0.0.0.0:9091/"
                                                 "result"}, \
            job_status_data

        last_request = json.loads(requests.get('http://0.0.0.0:9091/'
                                               'last_request').content)
        last_request['data'].pop('requested_timestamp')
        last_request['data'].pop('finished_timestamp')

        assert last_request[u'headers'][u'Content-Type'] == u'application/json'
        assert last_request[u'data'] == {
                    "status": "complete",
                    "sent_data": {"time": 0.1},
                    "job_id": "with_result",
                    "job_type": "example",
                    "result_url": "http://0.0.0.0:9091/"
                                  "result",
                    "error": None,
                    "data": "Slept for 0.1 seconds.",
                    "metadata": {'key': 'value'}
                    }, last_request

    def test_missing_job_id(self):
        rv = app.get('/job/not_there')
        assert rv.status_code == 404, rv.status
        error = json.loads(rv.data)
        assert error == {u'error': u'job_id not found'}

    def test_bad_metadata(self):
        rv = app.post(
            '/job/with_bad_metadata',
            data=json.dumps({"job_type": "example",
                             "data": {"time": 0.1},
                             "metadata": "meta",
                             "result_url": "http//0.0.0.0:9091/result",
                             "api_key": "key"}),
            content_type='application/json')

        return_value = json.loads(rv.data)
        assert return_value == {u'error': u'metadata has to be a '
                                          'json object'}, return_value

    def test_bad_url(self):
        rv = app.post(
            '/job/with_bad_result',
            data=json.dumps({"job_type": "example",
                             "data": {"time": 0.1},
                             "metadata": "meta",
                             "result_url": "ht//0.0.0.0:9091/resul",
                             "api_key": "key"}),
            content_type='application/json')

        return_value = json.loads(rv.data)
        assert return_value == {u'error': u'result_url has to start '
                                          'with http'}, return_value

    def test_zz_misfire(self):
        #has z because if this test failes will cause other tests to fail'''

        web.scheduler.misfire_grace_time = 0.000001
        rv = app.post(
            '/job/misfire',
            data=json.dumps({"job_type": "example",
                             "data": {"time": 0.1},
                             "metadata": {"moon": "moon",
                                          "nested": {"nested": "nested"},
                                          "key": "value"},
                             }),
            content_type='application/json')

        time.sleep(0.5)

        rv = app.get('/job/misfire')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')
        assert job_status_data == {u'status': u'error',
                                   u'sent_data': {u'time': 0.1},
                                   u'job_id': u'misfire',
                                   u'job_type': u'example',
                                   u'result_url': None,
                                   u'error': u'Job delayed too long, '
                                             'service full',
                                   u'data': None,
                                   u'metadata': {"key": "value",
                                                 "moon": "moon",
                                                 "nested": {"nested":
                                                            "nested"}}}, \
            job_status_data
        web.scheduler.misfire_grace_time = 3600

    def test_syncronous__raw_post(self):

        rv = app.post('/job/echoraw',
                      data=json.dumps({"metadata": {"key": "value",
                                                    "moo": "moo"},
                                       "job_type": "echo_raw",
                                       "data": "ping"}),
                      content_type='application/json')
        assert rv.data == 'ginp'

    def test_syncronous_post(self):

        rv = app.post('/job/echobasic',
                      data=json.dumps({"metadata": {"key": "value",
                                                    "moo": "moo"},
                                       "job_type": "echo", "data": "ping"}),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        return_data.pop('requested_timestamp')
        return_data.pop('finished_timestamp')

        assert return_data == {u'status': u'complete',
                               u'sent_data': u'ping',
                               u'job_id': u'echobasic',
                               u'job_type': u'echo',
                               u'result_url': None,
                               u'error': None,
                               u'data': u'>ping',
                               u'metadata': {"key": "value",
                                             "moo": "moo"}}, return_data

        rv = app.get('/job/echobasic')
        job_status_data = json.loads(rv.data)
        job_status_data.pop('requested_timestamp')
        job_status_data.pop('finished_timestamp')

        assert return_data == job_status_data

        rv = app.post('/job/echobasic',
                      data=json.dumps({"job_type": "echo", "data": "ping"}),
                      content_type='application/json')

        return_data = json.loads(rv.data)
        assert return_data == {u'error': u'job_id echobasic already exists'},\
            return_data

        rv = app.post('/job/echoknownbad',
                      data=json.dumps({"job_type": "echo", "data": ">ping"}),
                      content_type='application/json')
        return_data = json.loads(rv.data)
        return_data.pop('requested_timestamp')
        return_data.pop('finished_timestamp')
        assert return_data == {u'status': u'error',
                               u'sent_data': u'>ping',
                               u'job_id': u'echoknownbad',
                               u'job_type': u'echo',
                               u'result_url': None,
                               u'error': u'do not start message with >',
                               u'data': None,
                               u'metadata': {}}, return_data

        rv = app.post('/job/echounknownbad',
                      data=json.dumps({"job_type": "echo", "data": 1}),
                      content_type='application/json')
        return_data = json.loads(rv.data)
        assert 'AttributeError' in return_data['error']

        rv = app.post('/job/echobad_url',
                      data=json.dumps({"job_type": "echo",
                                       "data": "moo",
                                       "result_url": "http://bad_url"}),
                      content_type='application/json')
        return_data = json.loads(rv.data)
        return_data.pop('requested_timestamp')
        return_data.pop('finished_timestamp')
        assert return_data == {u'status': u'complete',
                               u'sent_data': u'moo',
                               u'job_id': u'echobad_url',
                               u'job_type': u'echo',
                               u'result_url': u'http://bad_url',
                               u'error': u'Process completed but unable to'
                                          ' post to result_url',
                               u'data': u'>moo',
                               u'metadata': {}}, return_data

    def test_z_test_list(self):
        #has z because needs some data to be useful

        rv = app.get('/job')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 11,  return_data['list']

        rv = app.get('/job?_limit=1')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 1,  return_data['list']

        rv = app.get('/job?_status=complete')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 7,  return_data['list']

        rv = app.get('/job?key=value')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 4,  return_data['list']

        rv = app.get('/job?key=value&moo=moo')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 2,  return_data['list']

        rv = app.get('/job?key=value&moo=moo&moon=moon')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 0,  return_data['list']

        rv = app.get('/job?key=value&moon=moon')
        return_data = json.loads(rv.data)
        assert len(return_data['list']) == 0,  return_data['list']
