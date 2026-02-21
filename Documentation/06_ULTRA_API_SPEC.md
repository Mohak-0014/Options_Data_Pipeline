
# 06 - Ultra Detailed API Interaction

## Authentication

1. Generate TOTP
2. Login via neo-api-client
3. Complete session_2fa
4. Store session token

## WebSocket

- Subscribe to 178 tickers
- Handle on_message
- Handle on_error
- Handle on_close

---

## Reconnect Strategy

If:
- No tick for 30 seconds
- on_close triggered

Then:
- Re-login
- Reconnect
- Re-subscribe
