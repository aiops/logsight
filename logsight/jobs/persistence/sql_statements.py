# noinspection SqlResolveForFile
SELECT_TABLE = """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name=%s"""

CREATE_TABLE = """CREATE TABLE %s(
                index varchar(512) NOT NULL,
                latest_ingest_time timestamp,
                PRIMARY KEY (index)
                )"""

# noinspection SqlResolve
UPDATE_TIMESTAMPS = """

INSERT INTO %s(index, latest_ingest_time) 
                        VALUES ('%s','%s')
                        ON CONFLICT (index) DO UPDATE 
                          SET latest_ingest_time = excluded.latest_ingest_time RETURNING *;"""
# noinspection SqlResolve
SELECT_ALL_INDEX = """SELECT index FROM %s"""

# noinspection SqlResolve
SELECT_FOR_INDEX = """SELECT * FROM %s WHERE index='%s'"""

SELECT_ALL = """SELECT * FROM %s"""
