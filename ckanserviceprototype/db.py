import sqlalchemy as sa

engine = None
metadata = None
task_table = None

def setup_db(db_url):
    global engine, metadata
    engine = sa.create_engine(db_url)
    metadata = sa.MetaData(engine)
    make_task_table()
    metadata.create_all(engine)

def make_task_table():
    global task_table
    task_table = sa.Table('datastorer_tasks', metadata,
       sa.Column('job_id', sa.UnicodeText, primary_key=True),
       sa.Column('job_type', sa.UnicodeText),
       sa.Column('status', sa.UnicodeText),
       sa.Column('data', sa.UnicodeText),
       sa.Column('error', sa.UnicodeText),
       sa.Column('metadata', sa.UnicodeText),
       sa.Column('requested_timestamp', sa.DateTime),
       sa.Column('finished_timestamp', sa.DateTime),
       sa.Column('sent_data', sa.UnicodeText),
       sa.Column('result_url', sa.UnicodeText),
       sa.Column('api_key', sa.UnicodeText),
    )
