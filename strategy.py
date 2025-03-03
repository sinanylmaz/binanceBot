import json
import time

import pandas as pd
import websockets
import ta

from Signal import Signals
from binanceAPI import create_order_buy, getMinuteData, create_order_sell, get_top_symbol, get_socket, getHistoricals, \
    get_socket_msg, get_symbol_info, get_symbol_ticker, get_account
from DBrepository import DBObject as dbo
from util import createFrameStream, applytechnicals, createFrame, livSMA

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
            df = getMinuteData(symbol, '1m', '30 min ago UTC')
            sincebuy = df.loc[df.index > pd.to_datetime(
                order['transactTime'], unit='ms')]
            if len(sincebuy) > 0:
                sincebuyret = (sincebuy.Open.pct_change() + 1).cumprod() - 1
                if sincebuyret[-1] > 0.0015 or sincebuyret[-1] < -0.0015:
                    order = create_order_sell(qty, symbol)
                    print(order)
                    break


def strategy(symbol, entry, lookback, qty, open_position=False):
    print('strategy')
    while True:
        df = db.read_coin(symbol)
        lookbackperiod = df.iloc[-lookback:]
        cumret = (lookbackperiod.Price.pct_change() + 1).cumprod() - 1
        if not open_position:
            if cumret[cumret.last_valid_index()] > entry:
                order = create_order_buy(symbol=symbol, qty=qty)
                print('alış' + str(cumret[cumret.last_valid_index()]))
                print(order)
                open_position = True
                break
    if open_position:
        while True:
            df = db.read_coin(symbol)
            sincebuy = df.loc[df.Time > pd.to_datetime(order['transactTime'], unit='ms')]
            if len(sincebuy) > 1:
                sincebuyret = (sincebuy.Price.pct_change() + 1).cumprod() - 1
                last_entry = sincebuyret[sincebuyret.last_valid_index()]
                if last_entry > 0.0015 or last_entry < -0.0015:
                    order = create_order_sell(symbol=symbol, qty=qty)
                    print('satiş' + str(last_entry))
                    print(order)
                    break


def strategyArtanStoploss(symbol, entry, lookback, qty, open_position=False):
    df = db.read_coin(symbol)
    df['Benchmark'] = df.Price.cummax()
    df['TSL'] = df.Benchmark * 0.95
    print('strategyArtanStoploss')
    while True:
        lookbackperiod = df.iloc[-lookback:]
        cumret = (lookbackperiod.Price.pct_change() + 1).cumprod() - 1
        if cumret[cumret.last_valid_index()] < entry:
            print('alış')
            order = create_order_buy(symbol=symbol, qty=qty)
            print('buy' + str(cumret[cumret.last_valid_index()]))
            print(order)
            open_position = True
            break
    if open_position:
        while True:
            df = db.read_artanStoploss_coin(symbol, pd.to_datetime(order['transactTime'], unit='ms'))
            df['Benchmark'] = df.Price.cummax()
            df['TSL'] = df.Benchmark * 0.995
            if df[df.Price < df.TSL].first_valid_index():
                order = create_order_sell(symbol=symbol, qty=qty)
                print('sell: ')
                print(order)
                break


async def strategyROC(symbol, qty, open_position=False):
    df = pd.DataFrame()
    stream = websockets.connect('wss://stream.binance.com:9443/stream?streams=bnbusdt@miniTicker')
    async with stream as receiver:
        while True:
            data = await receiver.recv()
            data = json.loads(data)['data']
            # her işlem olduğunda data alıyor
            df = df.append(createFrameStream(data))
            print('data alınıyor')
            if not open_position:
                # son 30 data roc işlemi uygulanır
                if len(df) > 30:
                    print('son 30 data alındı')
                    if ta.momentum.roc(df.Price, 30).iloc[-1] > 0 and ta.momentum.roc(df.Price, 30).iloc[-2]:
                        print('BUY' + str(ta.momentum.roc(df.Price, 30).iloc[-1]))
                        print(str(ta.momentum.roc(df.Price, 30).iloc[-2]))
                        open_position = True
                        order = create_order_buy(symbol=symbol, qty=qty)
                        # order = client.create_order(symbol='ADAUSDT', side='BUY', type='MARKET', quantity=50)
                        buy_price = float(order['fills'][0]['price'])  # float(df.iloc[-1].Price)#
                        buy_time = pd.to_datetime(df.iloc[-1].Time)
                        print('buyprice: ' + str(buy_price))
            if open_position:
                subdf = df[df.Time >= pd.to_datetime(order['transactTime'], unit='ms')]
                print('satıs: ' + str(len(subdf)))
                if len(subdf) > 1:
                    subdf['highest'] = subdf.Price.cummax()
                    subdf['trailngstop'] = subdf['highest'] * 0.995
                    if subdf.iloc[-1].Price < subdf.iloc[-1].trailngstop or df.iloc[-1].Price / float(
                        buy_price) > 1.002:
                        order = create_order_sell(symbol=symbol, qty=qty)
                        # order=client.create_order(symbol='ADAUSDT',side='SELL',type='MARKET',quantity=qty)
                        sell_price = float(df.iloc[-1].Price)  # order['fills'][0]['price']
                        print('satis: ' + str(sell_price))
                        open_position = False


def tradingStrategyMACD(symbol, qty, open_position=False):
    while True:
        df = getMinuteData(symbol, '1m', '60 min ago UTC')
        print('data alınıyor')
        if not open_position:
            print('getminutedata-1' + str(ta.trend.macd_diff(df.Close).iloc[-1]))
            print('getminutedata-2' + str(ta.trend.macd_diff(df.Close).iloc[-2]))
            if ta.trend.macd_diff(df.Close).iloc[-1] > 0 and ta.trend.macd_diff(df.Close).iloc[-2] < 0:
                order = create_order_buy(symbol=symbol, qty=qty)
                print('alis: ')
                print(order)
                open_position = True
                break
    if open_position:
        while True:
            df = getMinuteData(symbol, '1m', '60 min ago UTC')
            if ta.trend.macd_diff(df.Close).iloc[-1] < 0 and ta.trend.macd_diff(df.Close).iloc[-2] > 0:
                order = create_order_sell(symbol=symbol, qty=qty)
                print('satis: ')
                print(order)
                open_position = False
                break


def strategyStochRsiMacd(symbol, qty, open_position=False):
    df = getMinuteData(symbol, '1m', '100 min ago UTC')
    applytechnicals(df)
    inst = Signals(df, 25)
    inst.decide()
    print(f'current close is: ' + str(df.Close.iloc[-1]))
    if df.Buy.iloc[-1]:
        order = create_order_buy(symbol=symbol, qty=qty)
        print('buy: ')
        print(order)
        buyprice = float(order['fills'][0]['price'])
        open_position = True
    while open_position:
        time.sleep(0.5)
        df = getMinuteData(symbol, '1m', '2 min ago UTC')
        print(f'current close is: ' + str(df.Close.iloc[-1]))
        print(f'current target is: ' + str(buyprice * 1.005))
        print(f'current stop is: ' + str(buyprice * 0.995))
        if df.Close[-1] <= buyprice * 0.995 or df.Close[-1] >= 1.005 * buyprice:
            order = create_order_sell(symbol=symbol, qty=qty)
            print('satiş: ')
            print(order)
            break


async def strategy_highRisk(buy_amt, StopL=0.985, Target=1.02, open_position=False):
    try:
        asset = get_top_symbol()
    except:
        time.sleep(61)
        asset = get_top_symbol()
    # socket=get_socket(asset)
    df = getMinuteData(asset, '1m', '120 min ago UTC')
    # qty=round(buy_amt/df.Close.iloc[-1])
    qty = 1
    print(f'current close is: ' + str(df.Close.iloc[-1]) + '  coin: ' + asset)
    if ((df.Close.pct_change() + 1).cumprod()).iloc[-1] > 1:
        order = create_order_buy(symbol=asset, qty=qty)
        buyprice = float(order['fills'][0]['price'])
        print('alış: ' + str(buyprice))
        print(order)
        open_position = True
        while open_position:
            msg = await get_socket_msg(asset)
            df = createFrame(msg)
            print(f'current close is: ' + str(df.Price.values))
            print(f'current target is: ' + str(buyprice * Target))
            print(f'current stop is: ' + str(buyprice * StopL))
            if df.Price.values <= buyprice * StopL or df.Price.values >= Target * buyprice:
                order = create_order_sell(symbol=asset, qty=qty)
                print(order)
                break


async def strategy_alinabilecekCoinler(coin, qty, StopLoss, ST, LT, open_position=False):
    # websocket = await connect_to_dealer()
    # await listen_for_message(websocket)
    socket = await get_socket(coin)
    print('başlsangic')
    async with socket as tscm:
        while True:
            res = await tscm.recv()
            if res:
                frame = createFrame(res)
                print(frame)
                livest, livelt = livSMA(getHistoricals(coin, LT, ST), frame, ST, LT)
                if livest > livelt and not open_position:
                    order = create_order_buy(symbol=coin, qty=qty)
                    print(order)
                    buyprice = float(order['fills'][0]['price'])
                    open_position = True
                    if open_position:
                        print(f'current stop is: ' + str(buyprice * StopLoss))
                        if frame.Price[0] <= buyprice * StopLoss or frame.Price[0] >= 1.02 * buyprice:
                            order = create_order_sell(symbol=coin, qty=qty)
                            print(order)
                            # loop.stop()
                            break


async def satın_al(loocback):
    rets = []
    symbols = db.get_symbols()
    for symbol in symbols:
        prices = db.get_coin_from_db(symbol, loocback).Price
        cumret = (prices.pct_change() + 1).prod() - 1
        rets.append(cumret)
    top_coin = symbols[rets.index(max(rets))]
    investment_amt = 300
    info = get_symbol_info(top_coin)
    LotSize = float([i for i in info['filters'] if i['filterType'] == 'LOT_SIZE'][0]['minQty'])
    price = float(get_symbol_ticker(top_coin)['price'])
    buy_quantity = round(investment_amt / price, len(str(LotSize).split('.')[1]))
    free_usd = [i for i in get_account()['balances'] if i['asset'] == 'USDT'][0]['free']
    if float(free_usd) > investment_amt:
        order = create_order_buy(buy_quantity, top_coin)
        buy_price = float(order['fills'][0]['price'])
        print(order)
        await  sat_coin(top_coin, buy_price, buy_quantity)
    else:
        print('yeterli bakiye bulunamadı')
        quit()


async def sat_coin(symbol, buyprice, qty):
    socket = await get_socket(symbol)
    async with socket as tscm:
        while True:
            res = await tscm.recv()
            if res:
                frame = createFrame(res)
                print('satma için live data aokunuyor sürekli')
                print(frame)
                if frame.Price[0] <= buyprice * 0.97 or frame.Price[0] >= 1.02 * buyprice:
                    order = create_order_sell(symbol=symbol, qty=qty)
                    print(order)
                    # loop.stop()

    await close_connection()
