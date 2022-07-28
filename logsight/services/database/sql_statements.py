SELECT_TABLES = """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = 'public'"""

UPDATE_LOG_RECEIPT = """UPDATE logs_receipt 
                        SET processed_log_count = %s, status = %s
                        WHERE batch_id = %s
                        RETURNING *"""
