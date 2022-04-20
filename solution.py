from contextlib import suppress

import pandas as pd
import matplotlib.pyplot as plt

try:
    df_ = pd.read_pickle("df.pkl")
except FileNotFoundError:
    from data import df as df_


# Formatting
# ----
df = df_.copy()
df = df.apply(pd.to_numeric, errors="ignore", axis=0)
df["open time"] = pd.to_datetime(df["open time"], unit="ms")
df["close time"] = pd.to_datetime(df["close time"], unit="ms")


# Handle missing values
# ----
df = df.set_index("open time").resample("h").last()
missing = df.isnull().any(axis=1)
for key in missing[missing].index:
    iloc = df.index.get_loc(key)

    # NOTE: I'm interpreting the instructions as if I should get the immediate values
    # above and below, regardless of whether or not they are valid/non-NA (i.e.
    # consecutive missing will be ffilled with last value only)
    avg_close = df.iloc[[iloc - 1, iloc + 1]]["close"].mean()
    df.loc[key, "close"] = avg_close
    df.loc[key, "close time"] = pd.to_datetime(key.timestamp() + 3599, unit="s")

df.reset_index(inplace=True)
df["date"] = df["open time"].dt.date
df["spread"] = df["close"] - df["close spot"]


# Setup maturities/roll over dates
# ----
d0, dlast = df["open time"].dt.date.values[[0, -1]]
vertices = pd.date_range(d0, dlast, freq="W-FRI").to_series().resample("Q").last().dt.date
mask = df.date.isin(vertices)
df_mty = df.loc[mask[mask].index]

# Infer maturity bar
rollover_bars = {}
for date in vertices:
    rollover = df.loc[df.date == date, "spread"].diff().abs().idxmax() - 1
    if not pd.isna(rollover):
        rollover_bars[date] = rollover


# P&L Analysis
# ----
# Making a few assumptions for simplicity: i) spot is fully borrowed;
# ii) entering a future requires no collateral; and iii) no trading costs;
daily = pd.Series(0, index=df.index)
pnl_by_contract = {}
entry_bar = 0
for label, mty in rollover_bars.items():
    spread = df.spread.loc[entry_bar:mty]
    notional = df.loc[entry_bar, "close spot"]

    daily.loc[entry_bar:mty] = -(spread.diff().fillna(0)) / notional
    entry, exit = spread.iloc[[0, -1]]
    pnl_by_contract[label.strftime("%Y-%m-%d")] = (entry - exit) / notional
    entry_bar = mty + 1

pnl = daily.add(1).cumprod()


# Plotting
# ----
fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)

df.set_index("close time", inplace=True)
df.spread.plot(ax=ax1, title="Spread (hourly)")
for date in vertices:
    with suppress(KeyError):
        ax1.axvline(df.index[rollover_bars[date]], c="r", ls="--", lw=0.5)

pnl.index = df.index
pnl.plot(ax=ax2, title="Profit & Loss balance (hourly)")
