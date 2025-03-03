import numpy as np
import pandas as pd
from tqdm import tqdm

from DBApi import insert_data_frame, get_coin_from_db
from binanceAPI import get_exchange_info, get_historical_klines, get_multiplex_socket, close_connection
from util import createFrame, applytechnical


def getbestPerf():
    relevant = get_relevant()
    klines = {}
    for symbol in tqdm(relevant):
        klines[symbol] = get_historical_klines('1m', '1 hour ago UTC', symbol)
    return klines[symbol], relevant;


def get_relevant():
    info = get_exchange_info()
    symbols = [x['symbol'] for x in info['symbols']]
    exclude = ['UP', 'DOWN', 'BEAR', 'BULL']
    non_lev = [symbol for symbol in symbols if all(excludes not in symbol for excludes in exclude)]
    relevant = [symbol for symbol in non_lev if symbol.endswith('USDT')]
    return relevant


def bestPerf():
    returns, symbols = [], []
    klines, relevant = getbestPerf()
    for symbol in relevant:
        if len(klines) > 0:
            cumret = (pd.DataFrame(klines)[4].astype(float).pct_change() + 1).prod() - 1
            returns.append(cumret)
            symbols.append(symbol)
    return pd.DataFrame(returns, index=symbols, columns=['ret'])


async def alinabilecek_coinler():
    multi = [i.lower() + '@trade' for i in get_relevant()]
    ms = await get_multiplex_socket(multi)
    async with ms as tscm:
        while True:
            res = await tscm.recv()
            if res:
                frame = createFrame(res['data'])
                insert_data_frame(frame, frame.symbol[0])
                print(frame)
    await close_connection()
    # frame.to_sql(frame.symbol[0], engine, if_exists='append', index=False)


def qry(symbol):
    # now=dt.datetime.utcnow()
    # before = now - dt.timedelta(minutes=30)
    # qry_string = f'''SELECT E,c from binance."{symbol}" b WHERE b."Time" >= '{before}' '''
    df = get_coin_from_db(symbol, 30)
    df.E = pd.to_datetime(df.E)
    df = df.set_index('E')
    df = df.resample('1min').last
    applytechnical(df)
    df['position'] = np.where(df['SMA_7'] > df['SMA_25'], 1, 0)
    return df
