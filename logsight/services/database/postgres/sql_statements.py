READ_APPLICATION = """SELECT App.id, App.name, Users.key, App.status
                      FROM applications as App, users as Users
                      WHERE App.user_id = Users.id AND App.id=%s;"""

LIST_APPS = """SELECT App.id as application_id,App.status as application_status, App.name as application_name, 
                      Users.key as private_key
               FROM applications as App, users as Users
                WHERE App.user_id = Users.id;"""
