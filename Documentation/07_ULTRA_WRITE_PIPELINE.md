
# 07 - Ultra Detailed Write Pipeline

## Write Strategy

- 178 rows per 5-min window
- Single batch append call
- Never row-by-row writes

---

## Retry Logic

Exponential backoff:

1s
2s
4s
8s
16s

Max retries: 5

---

## Fallback

If retries fail:
- Save batch to unsent_backup.json
- Retry next cycle
