READ_APPLICATION = """SELECT App.id, App.name, Users.key
                        FROM applications as App,
                             users as Users
                        WHERE App.user_id = Users.id AND App.id=%s;"""

LIST_APPS = """SELECT App.id, App.name, Users.key
                FROM applications as App,
                     users as Users
                WHERE App.user_id = Users.id;"""
