
# 08 - Ultra Detailed Recovery & Backup Policy

## Checkpointing

Every 5 minutes:
- Save ATR state to local JSON
- Save last window timestamp

---

## Restart Logic

On startup:
- Load checkpoint
- Resume without recalculating history

---

## Monthly Backup

End of month:
- Export Google Sheet
- Save to external hard drive
- Create new monthly spreadsheet

---

## Integrity Enforcement

- 178 rows per timestamp
- No duplicate IDs
- No missing windows
- Append-only historical policy
