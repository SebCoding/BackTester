import sqlalchemy
from sqlalchemy_utils import database_exists

from Configuration import Configuration


class BaseDbData:
    URL_TEMPLATE = 'postgresql://<username>:<password>@<address>:<port>/<db_name>'

    def __init__(self, exchange_name):
        self.config = Configuration.get_config()
        # Database
        self.db_name = exchange_name.capitalize()
        self.db_url = self.get_db_url(self.db_name)
        self.validate_db()
        self.engine = sqlalchemy.create_engine(self.db_url)

    def validate_db(self):
        if not database_exists(self.db_url):
            raise Exception(f'{self.db_url} database does not exists.')

    def get_db_url(self, name):
        url = self.URL_TEMPLATE.replace('<db_name>', name)
        url = url.replace('<address>', self.config['database']['address'])
        url = url.replace('<port>', str(self.config['database']['port']))
        url = url.replace('<username>', self.config['database']['username'])
        url = url.replace('<password>', self.config['database']['password'])
        return url

    # Get the table name for this pair and interval
    @staticmethod
    def get_table_name(pair, interval):
        return f'Candles_{pair}_{interval}'

    def exec_sql_query(self, query):
        connection = self.engine.connect()
        result = connection.execute(query)
        # if result.rowcount > 0:
        #     for row in result:
        #         print(row)
        connection.close()
        return result
