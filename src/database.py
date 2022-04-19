import psycopg
from psycopg.rows import dict_row

from config import db_config


class Database:

    TABLE = db_config()['TABLE']['table']
    SELECT_ROW = db_config()['TABLE']['select_row']
    CURSOR = 'cursorMark'

    def __init__(self):
        try:
            query = f'SELECT {self.SELECT_ROW} FROM {self.TABLE}'

            self.conn = psycopg.connect(
                **db_config()['SYSTEM'],
                row_factory=dict_row
            )
            self.cursor = self.conn.cursor(name=self.CURSOR)
            self.cursor.execute(query)
        except psycopg.errors.UndefinedTable:
            print(f'[!!] Table does not exist \'{self.TABLE}\'')
            self.close()
        except psycopg.errors.Error as e:
            print(e)
            self.close()

    def close(self):
        if not self.cursor.closed and not self.conn.closed:
            self.cursor.close()
            self.conn.close()
        print(f'Close cursor: {self.cursor.closed}')
        print(f'Close database connection: {self.conn.closed}')

    def fetch_by_limit(self, limit=10):
        return self.cursor.fetchmany(limit)
