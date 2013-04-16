CKAN Service Provider
=====================

A simple flask app that makes functions available as synchronous or asynchronous jobs.

Routes
------

.. autoflask:: ckanserviceprovider.web:app
   :undoc-static:
   :include-empty-docstring:

Add a job
---------

Just decorate your function and it will become available as a job::

  import ckanserviceprovider.job as job
  import ckanserviceprovider.util as util

  @job.sync
  def echo(task_id, input):
      if input['data'].startswith('>'):
          raise util.JobError('do not start message with >')
      if input['data'].startswith('#'):
          raise Exception('serious exception')
      return '>' + input['data']

Expected job errors should be raised as `util.JobError`.
