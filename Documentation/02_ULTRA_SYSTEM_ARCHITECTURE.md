
# 02 - Ultra Detailed System Architecture

## 1. Architectural Philosophy

The system is designed as a real-time streaming ETL pipeline:

Data Source → Aggregation → Transformation → Persistence

All components must be decoupled.

---

## 2. Component Breakdown

### 2.1 Authentication Module
- Handles login
- Generates TOTP
- Refreshes session on expiry
- Retries on failure

### 2.2 WebSocket Client
- Maintains persistent connection
- Subscribes in batches
- Monitors heartbeat
- Detects silence > 30 seconds
- Triggers reconnect

### 2.3 Tick Buffer
Thread-safe in-memory structure:

{
  ticker: {
    window_start: datetime,
    open: float,
    high: float,
    low: float,
    close: float
  }
}

### 2.4 Candle Aggregator
- Aligns window to IST
- Finalizes on boundary
- Validates high ≥ open, close
- Validates low ≤ open, close

### 2.5 ATR Engine
Maintains:
{
  ticker: {
    prev_close: float,
    prev_atr: float
  }
}

### 2.6 Write Queue
Queue-based producer-consumer model.

### 2.7 Google Sheets Writer
- Batch append only
- Exponential backoff retry
- Local fallback storage

### 2.8 Checkpoint Manager
- Saves ATR state every 5 minutes
- Saves last processed window
- Reloads on restart

---

## 3. Threading Model

Thread 1: WebSocket Listener
Thread 2: Scheduler
Thread 3: Sheets Writer
