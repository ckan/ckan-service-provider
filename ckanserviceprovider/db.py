# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sqlalchemy as sa

engine = None
metadata = None
jobs_table = None
metadata_table = None
logs_table = None


def setup_db(app):
    global engine, metadata
    engine = sa.create_engine(app.config.get('SQLALCHEMY_DATABASE_URI'),
                              echo=app.config.get('SQLALCHEMY_ECHO'),
                              convert_unicode=True)
    metadata = sa.MetaData(engine)
    make_task_table()
    metadata.create_all(engine)


def make_task_table():
    global jobs_table, metadata_table, logs_table
    jobs_table = sa.Table('jobs', metadata,
                          sa.Column('job_id', sa.UnicodeText,
                                    primary_key=True),
                          sa.Column('job_type', sa.UnicodeText),
                          sa.Column('status', sa.UnicodeText,
                                    index=True),
                          sa.Column('data', sa.UnicodeText),
                          sa.Column('error', sa.UnicodeText),
                          sa.Column('requested_timestamp', sa.DateTime),
                          sa.Column('finished_timestamp', sa.DateTime),
                          sa.Column('sent_data', sa.UnicodeText),
                          # Callback url
                          sa.Column('result_url', sa.UnicodeText),
                          # CKAN API key
                          sa.Column('api_key', sa.UnicodeText),
                          # Key to administer job
                          sa.Column('job_key', sa.UnicodeText)
                          )

    metadata_table = sa.Table('metadata', metadata,
                              sa.Column('job_id',
                                        sa.ForeignKey("jobs.job_id", ondelete="CASCADE"),
                                        nullable=False, primary_key=True),
                              sa.Column('key', sa.UnicodeText,
                                        primary_key=True),
                              sa.Column('value', sa.UnicodeText,
                                        index=True),
                              sa.Column('type', sa.UnicodeText),
                              )

    logs_table = sa.Table('logs', metadata,
                          sa.Column('job_id',
                                    sa.ForeignKey("jobs.job_id", ondelete="CASCADE"),
                                    nullable=False),
                          sa.Column('timestamp', sa.DateTime),
                          sa.Column('message', sa.UnicodeText),
                          sa.Column('level', sa.UnicodeText),
                          sa.Column('module', sa.UnicodeText),
                          sa.Column('funcName', sa.UnicodeText),
                          sa.Column('lineno', sa.Integer)
                          )
