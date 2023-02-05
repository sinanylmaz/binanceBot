# This is a sample Python script.
from multiprocessing import Process

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from binanceAPI import getMinuteData, add_coin_from_socket
import asyncio

from repository import DBObject as dbo
from strategy import strategytest, strategy

def main():
    db = dbo()
    db.createSchema()

    # Use a breakpoint in the code line below to debug your script.
    strategy('BNBUSDT',0.001, 60, 0.001)
    #add_frame_db()
    #df = getMinuteData('BNBUSDT', '1m', '30m')
    #db.insert_data_frame(df=df,symbol=symbol)
    #print(df)
   # strategytest(symbol, 10,df)

async def add_from_socket():
    while True:
        add_coin_from_socket('BNBUSDT')
        #strategy('BNBUSDT', 0.001, 60, 0.001)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(add_from_socket())



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
