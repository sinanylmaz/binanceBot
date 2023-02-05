import pandas as pd
import psycopg2
from sqlalchemy import create_engine

import config


class DBObject():
    def __init__(self):
        self.conn = psycopg2.connect(
            database=config.DATABASE_CONFIG['database'],
            user=config.DATABASE_CONFIG['user'],
            password=config.DATABASE_CONFIG['password'],
            host=config.DATABASE_CONFIG['host'],
            port=config.DATABASE_CONFIG['port']
        )
        self.engine= create_engine("postgresql://postgres:12345@localhost/postgres")
        # establishing the connection
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def createSchema(self, schemanName='''binance'''):
        sql = '''CREATE SCHEMA IF NOT EXISTS %s''' % schemanName
        self.cursor.execute(sql)

    def insert_data_frame(self, df, symbol):
        df.to_sql(symbol, schema='binance',con=self.engine, if_exists='append',
                  index=False)


    def read_coin(self,symbol):
        return pd.read_sql('SELECT * FROM binance."'+symbol+'"',self.conn)


    def createCoinListTable(self):
        # EXECUTE THE INSERT QUERY
        self.cursor.execute(f'''
             CREATE TABLE IF NOT EXISTS  public.coinList (
             symbol text NOT NULL,
             PRIMARY KEY (symbol)
         ) ''')


    def insert_value(self, timestmp, open_, close_, high, low, vol, amount, tablename):
        # CREATE A CURSOR USING THE CONNECTION OBJECT

        # EXECUTE THE INSERT QUERY
        self.cursor.execute(f'''
            INSERT INTO
                binance.{tablename}(time_, open_,close_,high,low,vol,amount) 
            VALUES
                ('{timestmp}', '{open_}', '{close_}', '{high}', '{low}', '{vol}', '{amount}')
        ''')

    def insert2_coinList(self, symbol):
        # CREATE A CURSOR USING THE CONNECTION OBJECT

        # EXECUTE THE INSERT QUERY
        self.cursor.execute(f'''
               INSERT INTO
                   public.coinList(symbol) 
               VALUES
                   ('{symbol}')
           ''')

        # COMMIT THE ABOVE REQUESTS

    # Preparing query to create a database
    def getAllTableList(self, schema="""binance"""):
        self.cursor.execute("""SELECT * FROM public.coinList""")
        tables = [i[0] for i in self.cursor.fetchall()]
        return tables

    def create_table(self, tableName):
        print(tableName)
        # EXECUTE THE INSERT QUERY
        self.cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS  binance.{tableName} (
        time_ timestamp NULL,
        open_ float8 NULL,
        close_ float8 NULL,
        high float8 NULL,
        low float8 NULL,
        vol float8 NULL,
        amount float8 NULL
    ) ''')

    # engine = sqlalchemy.create_engine('sqlite:///BTCUSDTstream.db')

    def connClose(self):
        self.cursor.close()
        self.conn.close()
