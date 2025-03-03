# This is a sample Python script.
from multiprocessing import Process

from Signal import Signals
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import asyncio
import warnings

from binanceHelper import bestPerf, alinabilecek_coinler
from DBrepository import DBObject as dbo
from strategy import strategy, strategyArtanStoploss, strategyROC, tradingStrategyMACD, strategyStochRsiMacd, \
    strategy_highRisk, strategy_alinabilecekCoinler, satın_al
from util import applytechnicals


async def strtgy():
    warnings.filterwarnings('ignore')

    # Use a breakpoint in the code line below to debug your script.
    strategy('BNBUSDT', 0.001, 40, 1)
    strategyArtanStoploss('BNBUSDT', -0.0015, 60, 1)
    await strategyROC('BNBUSDT',1)
    tradingStrategyMACD('BNBUSDT',1)
    strategyStochRsiMacd('BNBUSDT',1)
    await strategy_highRisk(200)
    await strategy_alinabilecekCoinler('BNBUSDT',1,0.98,6,25)
    df=bestPerf()
    print(df)
    await alinabilecek_coinler()
    await satın_al(1)
    # print(df)


def main():
    loop = asyncio.get_event_loop()
    try:
        db = dbo()
        db.createSchema()
        while True:
            loop.run_until_complete(strtgy())
            # strtgy()
    except Exception as e:
        print('main exception: ')
        print(e)
        loop.stop()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
