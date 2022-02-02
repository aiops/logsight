READ_APPLICATION = """SELECT App.id, App.name, Users.elasticsearch_key
                      FROM applications as App, users as Users
                      WHERE App.user_id = Users.id AND App.id=%s;"""

LIST_APPS = """SELECT App.id as application_id, App.name as application_name, Users.elasticsearch_key as private_key
               FROM applications as App, users as Users
                WHERE App.user_id = Users.id;"""
