import binance
import pandas as pd

client = binance.Client()

hour = 60 * 60 * 1000
start_epoch = 1612339200000
end_epoch = start_epoch + (hour * 1500) - 1

r = client.futures_continous_klines(
    pair="BTCUSDT",
    contractType="CURRENT_QUARTER",
    interval="1h",
    limit=1500,
    startTime=start_epoch,
    endTime=end_epoch,
)
df_future = pd.DataFrame(
    r,
    columns=[
        "open time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close time",
        "qav",
        "number of trades",
        "tbv",
        "tbqav",
        "ignore",
    ],
)


for i in range(0, 6):
    start_epoch = end_epoch + 1
    end_epoch = start_epoch + (hour * 1500) - 1
    r = client.futures_continous_klines(
        pair="BTCUSDT",
        contractType="CURRENT_QUARTER",
        interval="1h",
        limit=1500,
        startTime=start_epoch,
        endTime=end_epoch,
    )
    df_temp = pd.DataFrame(
        r,
        columns=[
            "open time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close time",
            "qav",
            "number of trades",
            "tbv",
            "tbqav",
            "ignore",
        ],
    )
    df_future = df_future.append(df_temp)


klines = client.get_historical_klines(
    "BTCUSDT", client.KLINE_INTERVAL_1HOUR, "3 Feb, 2021 8 am", "15 Apr, 2022"
)
df_spot = pd.DataFrame(
    klines,
    columns=[
        "open time",
        "open",
        "high",
        "low",
        "close spot",
        "volume",
        "close time",
        "qav",
        "number of trades",
        "tbv",
        "tbqav",
        "ignore",
    ],
)

df_spot = df_spot[["open time", "close spot"]]
df = pd.merge(df_future, df_spot, on="open time")

df.to_pickle("df.pkl")
