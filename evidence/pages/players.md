---
title: Player Statistics
---

# Player Season Statistics

```sql player_stats
SELECT * FROM nba.player_stats
ORDER BY pts DESC
LIMIT 100
```

## Top Scorers

<DataTable data={player_stats} rows=25 search=true/>
