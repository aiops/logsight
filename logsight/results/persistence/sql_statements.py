# noinspection SqlResolveForFile
SELECT_TABLE = """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name=%s"""

CREATE_TABLE = """CREATE TABLE %s(
                index varchar(512) NOT NULL,
                latest_ingest_time timestamp,
                latest_processed_time timestamp,
                PRIMARY KEY (index),
                CONSTRAINT fk_index FOREIGN KEY (index)
                  REFERENCES users (key)
                  ON DELETE CASCADE
                )"""

# noinspection SqlResolve
UPDATE_TIMESTAMPS = """

INSERT INTO %s(index, latest_ingest_time, latest_processed_time) 
                        VALUES ('%s','%s','%s')
                        ON CONFLICT (index) DO UPDATE 
                          SET latest_ingest_time = excluded.latest_ingest_time, 
                              latest_processed_time = excluded.latest_processed_time RETURNING *;"""
# noinspection SqlResolve
SELECT_ALL_INDEX = """SELECT index FROM %s"""

SELECT_ALL_USER_INDEX = """SELECT key FROM users"""

# noinspection SqlResolve
SELECT_FOR_INDEX = """SELECT * FROM %s WHERE index='%s'"""

SELECT_ALL = """SELECT * FROM %s"""
