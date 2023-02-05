from binance.client import Client
import config
import pandas as pd
from binance import BinanceSocketManager

from repository import DBObject as dbo
from util import createFrame

API_KEY = config.BINANCE['api_key']
SECRET_KEY = config.BINANCE['api_secret']
client = Client(API_KEY, SECRET_KEY, testnet=True)
bsm = BinanceSocketManager(client)
symbol = 'BNBUSDT'
socket = bsm.trade_socket(symbol)
db = dbo()


async def get_socket():
    await socket.__aenter__()
    msg = await socket.recv()
    return msg


def add_historical_klines():
    df = getMinuteData(symbol, '1m', '30m')
    db.insert_data_frame(df, symbol)

async def add_coin_from_socket(symbol):
    msg = await get_socket()
    print(msg)
    data_frame = createFrame(msg)
    db.insert_data_frame(data_frame, symbol)


def getMinuteData(symbol, interval, lookback):
    frame = pd.DataFrame(client.get_historical_klines(symbol, interval, lookback + " min ago UTC"))
    frame = frame.iloc[:, :6]
    frame.columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    frame = frame.set_index('Time')
    frame.index = pd.to_datetime(frame.index, unit='ms')
    frame = frame.astype(float)
    return frame


def create_order_sell(qty, symbol):
    order = client.create_order(symbol=symbol,
                                side='SELL', type='MARKET',
                                quantity=qty)
    return order


def create_order_buy(qty, symbol):
    order = client.create_order(symbol=symbol,
                                side='BUY', type='MARKET',
                                quantity=qty)
    return order
