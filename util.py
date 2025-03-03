import pandas as pd
import ta


def createFrame(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'p']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df

def createFrameStream(msg):
    df = pd.DataFrame([msg])
    df = df.loc[:, ['s', 'E', 'c']]
    df.columns = ['symbol', 'Time', 'Price']
    df.Price = df.Price.astype(float)
    df.Time = pd.to_datetime(df.Time, unit='ms')
    return df

def applytechnicals(df):
    df['%K']=ta.momentum.stoch(df.High,df.Low,df.Close,window=14,smooth_window=3)
    df['%D']=df['%K'].rolling(3).mean()
    df['rsi']=ta.momentum.rsi(df.Close,window=14)
    df['macd']=ta.trend.macd_diff(df.Close)
    df.dropna(inplace=True)


def livSMA(hist, live,ST,LT):
    liveST = (hist['ST'].values + live.Price.values) / ST
    liveLT = (hist['LT'].values + live.Price.values) / LT
    return liveST, liveLT

def applytechnical(df):
    df['SMA_7']=df.c.rolling(7).mean()
    df['SMA_25'] = df.c.rolling(25).mean()
    df.dropna(inplace=True)