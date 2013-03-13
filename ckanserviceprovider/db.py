import sqlalchemy as sa

engine = None
metadata = None
jobs_table = None
metadata_table = None


def setup_db(app):
    global engine, metadata
    engine = sa.create_engine(app.config.get('SQLALCHEMY_DATABASE_URI'),
                              echo=app.config.get('SQLALCHEMY_ECHO'))
    metadata = sa.MetaData(engine)
    make_task_table()
    metadata.create_all(engine)


def make_task_table():
    global jobs_table, metadata_table
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
                          sa.Column('result_url', sa.UnicodeText),
                          sa.Column('api_key', sa.UnicodeText),
                          )

    metadata_table = sa.Table('metadata', metadata,
                              sa.Column('job_id', sa.UnicodeText,
                                        primary_key=True),
                              sa.Column('key', sa.UnicodeText,
                                        primary_key=True),
                              sa.Column('value', sa.UnicodeText,
                                        index=True),
                              sa.Column('type', sa.UnicodeText),
                              )
