# noinspection SqlResolveForFile
SELECT_TABLE = """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = 'public' AND table_name=%s"""

CREATE_TABLE = """CREATE TABLE %s(
                index varchar(512) NOT NULL,
                start_date timestamp,
                end_date timestamp,
                PRIMARY KEY (index),
                CONSTRAINT fk_index FOREIGN KEY (index)
                  REFERENCES applications (index)
                  ON DELETE CASCADE
                )"""

# noinspection SqlResolve
UPDATE_TIMESTAMPS = """

INSERT INTO %s(index, start_date, end_date) 
                        VALUES ('%s','%s','%s')
                        ON CONFLICT (index) DO UPDATE 
                          SET start_date = excluded.start_date, 
                              end_date = excluded.end_date RETURNING *;"""
# noinspection SqlResolve
SELECT_ALL_INDEX = """SELECT index FROM %s"""

SELECT_ALL_APP_INDEX = """SELECT index FROM applications"""
# noinspection SqlResolve
SELECT_FOR_INDEX = """SELECT * FROM %s WHERE index='%s'"""

SELECT_ALL = """SELECT * FROM %s"""
