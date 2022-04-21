READ_APPLICATION = """SELECT App.id, App.name, Users.key, App.status, App.application_key
                      FROM applications as App, users as Users
                      WHERE App.user_id = Users.id AND App.id=%s;"""

LIST_APPS = """SELECT App.id as application_id,App.status as application_status, App.name as application_name,
                       App.application_key as application_key, Users.key as private_key
               FROM applications as App, users as Users
                WHERE App.user_id = Users.id;"""

SELECT_DATABASES = """SELECT datname FROM pg_database;"""

SELECT_TABLES = """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = 'public'"""
