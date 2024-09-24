import numpy as np
import pandas as pd
import pandas_ta as ta


def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = true_range.rolling(window=period).mean()
    return atr


def calculate_signals(df, a=2, c=1):
    df.ta.ema(close=df['close'], length=20, append=True)
    df['atr'] = calculate_atr(df, period=c)
    df['nLoss'] = a * df['atr']
    df['src'] = df['close']
    df['xATRTrailingStop'] = np.nan

    # Initialize xATRTrailingStop
    if len(df) > 0:
        df.loc[df.index[0], 'xATRTrailingStop'] = df.loc[df.index[0], 'src']
    for i in range(1, len(df)):
        if df.iloc[i]['src'] > df.iloc[i - 1]['xATRTrailingStop'] and df.iloc[i - 1]['src'] > \
                df.iloc[i - 1]['xATRTrailingStop']:
            df.loc[df.index[i], 'xATRTrailingStop'] = max(df.iloc[i - 1]['xATRTrailingStop'],
                                                          df.iloc[i]['src'] - df.iloc[i]['nLoss'])
        elif df.iloc[i]['src'] < df.iloc[i - 1]['xATRTrailingStop'] and df.iloc[i - 1]['src'] < \
                df.iloc[i - 1]['xATRTrailingStop']:
            df.loc[df.index[i], 'xATRTrailingStop'] = min(df.iloc[i - 1]['xATRTrailingStop'],
                                                          df.iloc[i]['src'] + df.iloc[i]['nLoss'])
        elif df.iloc[i]['src'] > df.iloc[i - 1]['xATRTrailingStop']:
            df.loc[df.index[i], 'xATRTrailingStop'] = df.iloc[i]['src'] - df.iloc[i]['nLoss']
        else:
            df.loc[df.index[i], 'xATRTrailingStop'] = df.iloc[i]['src'] + df.iloc[i]['nLoss']

    df['pos'] = np.where((df['src'].shift(1) < df['xATRTrailingStop'].shift(1)) & (df['src'] > df['xATRTrailingStop']),
                         1, np.where(
            (df['src'].shift(1) > df['xATRTrailingStop'].shift(1)) & (df['src'] < df['xATRTrailingStop']), -1, np.nan))
    df['pos'] = df['pos'].ffill().fillna(0)
    return df

