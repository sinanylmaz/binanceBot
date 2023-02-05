import pandas as pd

from binanceAPI import create_order_buy, getMinuteData, create_order_sell
from repository import DBObject as dbo

db = dbo()
def strategytest(symbol, qty, df, entried=False):
    # df=getMinuteData('BTCUSDT','1m','30')
    cumulret = (df.Open.pct_change() + 1).cumprod() - 1
    if not entried:
        print(cumulret)
        if cumulret[-1] > -0.002:
            order = create_order_buy(qty, symbol)
            print(order)
            entried = True
        else:
            print('No trade has been execute')
    if entried:
        while True:
            df = getMinuteData(symbol, '1m', '30m')
            sincebuy = df.loc[df.index > pd.to_datetime(
                order['transactTime'], unit='ms')]
            if len(sincebuy) > 0:
                sincebuyret = (sincebuy.Open.pct_change() + 1).cumprod() - 1
                if sincebuyret[-1] > 0.0015 or sincebuyret[-1] < -0.0015:
                    order = create_order_sell(qty, symbol)
                    print(order)
                    break


def strategy(symbol,entry, lookback, qty, open_position=False):
    while True:
        df = db.read_coin(symbol)
        lookbackperiod = df.iloc[-lookback:]
        cumret = (lookbackperiod.Price.pct_change() + 1).cumprod() - 1
        if not open_position:
            if cumret[cumret.last_valid_index()] > entry:
                order = create_order_buy(symbol=symbol, side='BUY', type='MARKET', quantity=qty)
                print('alış' + str(cumret[cumret.last_valid_index()]))
                open_position = True
                break
        if open_position:
            while True:
                df = db.read_coin(symbol)
                sincebuy = df.loc[df.Time > pd.to_datetime(order['transactTime'], uni='ms')]
                print('satiş1' + str(sincebuy))
                if len(sincebuy) > 1:
                    sincebuyret = (sincebuy.Price.pct_change() + 1).cumprod() - 1
                    last_entry = sincebuyret[sincebuyret.last_valid_index()]
                    print('satiş2' + str(last_entry))
                    if last_entry > 0.0015 or last_entry < -0.0015:
                        order = create_order_sell(symbol=symbol, side='SELL', type='MARKET', quantity=qty)
                        print('satiş' + str(last_entry))
                        break



