# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import datetime
import json

import sqlalchemy


# Some module-global constants. These are accessed directly by other modules.
# It would be good to factor these out.
engine = None
metadata = None
jobs_table = None
metadata_table = None
logs_table = None


def init(uri, echo=False):
    """Initialise the database.

    Initialise the sqlalchemy engine, metadata and table objects that we use to
    connect to the database.

    Create the database and the database tables themselves if they don't
    already exist.

    :param uri: the sqlalchemy database URI
    :type uri: string

    :param echo: whether or not to have the sqlalchemy engine log all
        statements to stdout
    :type echo: bool

    """
    global engine, metadata, jobs_table, metadata_table, logs_table
    engine = sqlalchemy.create_engine(uri, echo=echo, convert_unicode=True)
    metadata = sqlalchemy.MetaData(engine)
    jobs_table = _init_jobs_table()
    metadata_table = _init_metadata_table()
    logs_table = _init_logs_table()
    metadata.create_all(engine)


def drop_all():
    """Delete all the database tables (if they exist).

    This is for tests to reset the DB. Note that this will delete *all* tables
    in the database, not just tables created by this module (for example
    apscheduler's tables will also be deleted).

    """
    if metadata:
        metadata.drop_all(engine)


def add_pending_job(job_id, job_key, job_type, api_key,
                    data=None, metadata=None, result_url=None):
    """Add a job with status "pending" to the jobs table.

    All code that adds jobs to the jobs table should go through this function.
    Code that adds to the jobs table manually should be refactored to use this
    function.

    May raise unspecified exceptions from Python core, SQLAlchemy or JSON!
    TODO: Document and unit test these!

    :param job_id: a unique identifier for the job, used as the primary key in
        ckanserviceprovider's "jobs" database table
    :type job_id: unicode

    :param job_key: the "key to administer the job" (?)
    :type job_key: unicode

    :param api_key: the client site API key that ckanserviceprovider will use
        when posting the job result to the result_url
    :type api_key: unicode

    :param data: I'm guessing this is the input data for the job, sent by
        the client to ckanserviceprovider when submitting the job request
    :type data: JSON-encodable dict

    :param metadata: A dict of arbitrary (key, value) metadata pairs to be
        stored along with the job. The keys should be strings, the values can
        be strings or any JSON-encodable type.
        Not sure what this metadata is for?
    :type metadata: dict

    :param result_url: the callback URL that ckanserviceprovider will post the
        job result to when the job has finished
    :type result_url: unicode


    """
    if not data:
        data = {}

    if not metadata:
        metadata = {}

    conn = engine.connect()
    trans = conn.begin()
    try:
        conn.execute(jobs_table.insert().values(
            job_id=job_id,
            job_type=job_type,
            status='pending',
            requested_timestamp=datetime.datetime.now(),
            sent_data=json.dumps(data),
            result_url=result_url,
            api_key=api_key,
            job_key=job_key))

        # Insert any (key, value) metadata pairs that the job has into the
        # metadata table.
        inserts = []
        for key, value in metadata.items():
            type_ = 'string'
            if not isinstance(value, basestring):
                value = json.dumps(value)
                type_ = 'json'
            inserts.append(
                {"job_id": job_id,
                 "key": key,
                 "value": value,
                 "type": type_}
            )
        if inserts:
            conn.execute(metadata_table.insert(), inserts)
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()


def mark_job_as_completed(job_id, data=None):
    """Mark a job as completed successfully.

    This also deletes the API key (that ckanserviceprovider uses when posting
    the job result to the result_url) from the jobs table, so that we don't
    have lots of unneeded API keys lying around in our DB being a security
    issue.

    """
    status = "complete"
    finished_timestamp = datetime.datetime.now()
    engine.execute(
        jobs_table.update()
        .where(jobs_table.c.job_id == job_id)
        .values(status=status, finished_timestamp=finished_timestamp,
                api_key=None, data=data))


def _init_jobs_table():
    """Initialise the jobs_table object."""
    _jobs_table = sqlalchemy.Table(
        'jobs', metadata,
        sqlalchemy.Column('job_id', sqlalchemy.UnicodeText, primary_key=True),
        sqlalchemy.Column('job_type', sqlalchemy.UnicodeText),
        sqlalchemy.Column('status', sqlalchemy.UnicodeText, index=True),
        sqlalchemy.Column('data', sqlalchemy.UnicodeText),
        sqlalchemy.Column('error', sqlalchemy.UnicodeText),
        sqlalchemy.Column('requested_timestamp', sqlalchemy.DateTime),
        sqlalchemy.Column('finished_timestamp', sqlalchemy.DateTime),
        sqlalchemy.Column('sent_data', sqlalchemy.UnicodeText),
        # Callback URL:
        sqlalchemy.Column('result_url', sqlalchemy.UnicodeText),
        # CKAN API key:
        sqlalchemy.Column('api_key', sqlalchemy.UnicodeText),
        # Key to administer job:
        sqlalchemy.Column('job_key', sqlalchemy.UnicodeText),
        )
    return _jobs_table


def _init_metadata_table():
    """Initialise the metadata_table object."""
    _metadata_table = sqlalchemy.Table(
        'metadata', metadata,
        sqlalchemy.Column(
            'job_id', sqlalchemy.ForeignKey("jobs.job_id", ondelete="CASCADE"),
            nullable=False, primary_key=True),
        sqlalchemy.Column('key', sqlalchemy.UnicodeText, primary_key=True),
        sqlalchemy.Column('value', sqlalchemy.UnicodeText, index=True),
        sqlalchemy.Column('type', sqlalchemy.UnicodeText),
        )
    return _metadata_table


def _init_logs_table():
    """Initialise the logs_table object."""
    _logs_table = sqlalchemy.Table(
        'logs', metadata,
        sqlalchemy.Column(
            'job_id', sqlalchemy.ForeignKey("jobs.job_id", ondelete="CASCADE"),
            nullable=False),
        sqlalchemy.Column('timestamp', sqlalchemy.DateTime),
        sqlalchemy.Column('message', sqlalchemy.UnicodeText),
        sqlalchemy.Column('level', sqlalchemy.UnicodeText),
        sqlalchemy.Column('module', sqlalchemy.UnicodeText),
        sqlalchemy.Column('funcName', sqlalchemy.UnicodeText),
        sqlalchemy.Column('lineno', sqlalchemy.Integer)
        )
    return _logs_table
