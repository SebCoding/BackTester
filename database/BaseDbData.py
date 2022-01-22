import sqlalchemy
from sqlalchemy_utils import database_exists

class BaseDbData:
    DB_URL = 'postgresql://CryptoMakerUser:XF4BruF,fxnWUcs@localhost:5432/<db_name>'

    def __init__(self, exchange_name):
        # Database
        self.db_name = exchange_name.capitalize()
        self.db_url = self.DB_URL.replace('<db_name>', self.db_name)
        self.validate_db_name()
        self.engine = sqlalchemy.create_engine(self.db_url)


    def validate_db_name(self):
        if not database_exists(self.db_url):
            raise Exception(f'{self.db_url} database does not exists.')

    # Get the table name for this pair and interval
    @staticmethod
    def get_table_name(pair, interval):
        return f'Candles_{pair}_{interval}'

    def exec_sql_query(self, query):
        connection = self.engine.connect()
        result = connection.execute(query)
        if result.rowcount > 0:
            for row in result:
                print(row)
        connection.close()
