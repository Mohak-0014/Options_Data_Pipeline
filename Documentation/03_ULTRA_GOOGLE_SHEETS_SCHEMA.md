
# 03 - Ultra Detailed Google Sheets Schema

Spreadsheet Naming Convention:
Kotak_Volatility_YYYY_MM

---

## Sheet: market_data

Columns:

1. id (STRING, PRIMARY KEY)
2. timestamp (DATETIME IST)
3. ticker (STRING)
4. segment (STRING)
5. open (FLOAT)
6. high (FLOAT)
7. low (FLOAT)
8. close (FLOAT)
9. tr (FLOAT)
10. atr (FLOAT)
11. volume (FLOAT OPTIONAL)
12. created_at (DATETIME)

Append-only policy enforced.

---

## Sheet: atr_state

1. ticker
2. last_close
3. last_atr
4. last_timestamp
5. updated_at

Exactly 178 rows.

---

## Sheet: system_log

1. timestamp
2. level (INFO/WARNING/ERROR)
3. event
4. window
5. details

---

## Sheet: metadata

Stores:
- schema_version
- atr_period
- timezone
- tickers_count
