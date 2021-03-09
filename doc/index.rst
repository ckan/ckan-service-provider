CKAN Service Provider
=====================

A simple flask app that makes functions available as synchronous or asynchronous jobs.

Routes
------

.. autoflask:: ckanserviceprovider.web:app
   :undoc-static:
   :include-empty-docstring:

Administration
--------------

To view the results of a job or resubmit it, the job key, that is returned when a job is created,
is needed. Alternatively, you can log in as admin or provide the secure key. The credentials for
the admin user and the secure key stored in the settings file.

Add a job
---------

Just decorate your function and it will become available as a job::

  import ckanserviceprovider.job as job
  import ckanserviceprovider.util as util

  @job.synchronous
  def echo(task_id, input):
      handler = util.StoringHandler(task_id, input)
      logger = logging.getLogger(__name__)
      logger.addHandler(handler)

      if input['data'].startswith('>'):
          raise util.JobError('do not start message with >')
      if input['data'].startswith('#'):
          raise Exception('serious exception')
      if input['data'].startswith('&'):
        logger.warn('just a warning')
      return '>' + input['data']

Expected job errors should be raised as `util.JobError`. For logging, use the handler
``util.StoringHandler`` to make sure that the logs are properly saved.

