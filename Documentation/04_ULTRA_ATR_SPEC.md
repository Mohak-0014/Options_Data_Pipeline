
# 04 - Ultra Detailed ATR Specification

## True Range Formula

TR = max(
    high - low,
    abs(high - prev_close),
    abs(low - prev_close)
)

## Initialization

First ATR:
Mean of first 14 TR values.

Subsequent ATR:
ATR = ((prev_atr * 13) + TR) / 14

Precision:
Round to 4 decimal places.

---

## Validation Rules

- ATR must never be negative.
- ATR must not jump > 3x previous ATR without large TR spike.
