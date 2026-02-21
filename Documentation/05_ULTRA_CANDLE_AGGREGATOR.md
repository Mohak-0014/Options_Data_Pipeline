
# 05 - Ultra Detailed Candle Aggregator

## Window Alignment

Windows aligned strictly to IST 5-minute boundaries.

Valid times:
09:20
09:25
09:30
...
15:30

No modulo logic allowed.

---

## Aggregation Rules

For first tick:
open = price

For each tick:
high = max(high, price)
low = min(low, price)
close = price

---

## Finalization

At boundary:
- Validate 178 tickers present
- Log missing ticker if any
- Send batch to ATR engine
