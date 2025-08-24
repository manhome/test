Risk Management
    - Initial_Stop (from Reference Date or Reference Price) [suppose WTD, so Reference Date = Friday]
    - Trailing_Stop (Assign before market open)
    - Limit_Price = max(Initial_Stop, Trailing_Stop)
    - Port_Weight(t-1) = Sec_MV_HKD / Equity_MV_HKD
    - Active Weight(t-1) = Port_Weight(t-1) - Bmk_Weight(t-1)
    - Active Position = Active Weight(t-1) * Equity_MV_HKD / Price_HKD(t-1)
    - For Active Position > 0, Set Sell Limit Order for Exit
      For Active Position < 0, Set Buy Limit Order for Exit
    - Round_Lot = 1 share in backtest; (Trade Round Lot in practice)
    - Use RRG-like (e.g. smoothened relative strength) to determine the cycle of specific stocks
    - aka Momentum strategy (Buy Winners, Sell Losers, set aside in uncertainty)
    - To avoid sector bias, try to maintain sector-neutral

Back-test Simulation:
    Period = 2023-12-31 to 2025-02-28
    Initial_Cap = 1 mio (HKD)
    TCost = 0.30%
    Universe = CSIHK100
    Active Bet = +/- 0.1%

    Download OHLCV (split-adjusted)

Choose Parameters for RRG:
short-period to minimize MSE (Mean Square Error) in 3-month daily samples

--------------------------------------------------------------------------------
RRG
===
Relative Strength (RS) = Price / Bmk
RS_Ratio = SMA_10(RS) / SMA_30(RS) [i.e. (Fast, Slow) = (10, 30)]
RS_Momentum = RS_Ratio / SMA_9(RS_Ratio) [i.e. (Fast, Slow, Signal) = (10, 30, 9)] (quite similar to MACD)

Suppose Origin = 1

Interpretation:
RS_Ratio > 1 => short-average of RS > long-average of RS => increasing against Bmk
RS_Ratio < 1 => short-average of RS < long-average of RS => decreasing against Bmk

RS_Momentum > 1 => similar to MACD above Signal_Line => +ve momentum
RS_Momentum < 1 => similar to MACD below Signal_Line => -ve momentum

Log version:
RS = log(Price) - log(Bmk)

SMA_10(RS) = 1/10*[SUM(log(Price)) - SUM(log(Bmk))]
SMA_30(RS) = 1/30*[SUM(log(Price)) - SUM(log(Bmk))]
RS_Ratio = Exp(SMA_10(RS) - SMA_30(RS))

SMA_9(RS_Ratio) = 1/9*SUM(log(RS_Ratio))
RS_Momentum = Exp(RS_Ratio - SMA_9(RS_Ratio))

--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
