
# 01 - Ultra Detailed Product Requirements Document (PRD)

## 1. Executive Summary
The Kotak Neo 5-Minute Volatility Harvester is a production-grade, fault-tolerant,
real-time financial data ingestion and volatility computation engine designed
to support real-money options trading decisions.

Primary Function:
- Collect tick-level market data from Kotak Neo WebSocket
- Aggregate into strict 5-minute OHLC candles
- Compute 14-period ATR
- Persist to Google Sheets as primary datastore
- Guarantee zero missing candles during market hours

Deployment Target:
- 24/7 Linux VPS
- IST timezone alignment

Document Generated: 2026-02-20 18:27:58 UTC

---

## 2. Business Objective

This system is NOT a trading bot.
It is a volatility data infrastructure layer.

The output supports:
- Option pricing decisions
- Volatility regime detection
- Intraday ATR-based strategy filtering

Failure tolerance must approach institutional standards.

---

## 3. Operating Window

Market Hours:
09:15:00 IST – 15:30:00 IST

System Behavior:
- Start WebSocket before 09:15
- Strictly finalize last candle at 15:30
- Gracefully disconnect
- Save checkpoint
- Idle until next session

---

## 4. Functional Requirements

FR1: Connect to Kotak Neo WebSocket using valid session.
FR2: Subscribe to 178 instruments.
FR3: Maintain continuous tick ingestion.
FR4: Aggregate tick data into 5-minute OHLC candles aligned to IST boundaries.
FR5: Compute ATR using standard recursive formula.
FR6: Batch-write 178 rows to Google Sheets per window.
FR7: Maintain ATR state persistence across restarts.
FR8: Implement reconnect logic for WebSocket and API failures.
FR9: Log all system events.
FR10: Prevent duplicate writes.

---

## 5. Non-Functional Requirements

- Data Integrity > 99.999%
- Write completion < 30 seconds
- No blocking in WebSocket callback
- Memory usage bounded to active window
- Deterministic time alignment
