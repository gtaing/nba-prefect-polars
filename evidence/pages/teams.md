---
title: Team Statistics
---

# Team Season Statistics

```sql seasons
SELECT DISTINCT season FROM nba.team_stats ORDER BY season DESC
```

<Dropdown name=season_filter data={seasons} value=season title="Select Season">
  <DropdownOption value="%" valueLabel="All Seasons"/>
</Dropdown>

```sql team_standings
SELECT
  team_name,
  season,
  wins,
  losses,
  total_games,
  ROUND(wins * 100.0 / total_games, 1) as win_pct,
  ROUND(team_pts, 1) as ppg,
  ROUND(opponent_pts, 1) as opp_ppg,
  ROUND(team_pts - opponent_pts, 1) as point_diff
FROM nba.team_stats
WHERE season::VARCHAR LIKE '${inputs.season_filter.value}'
ORDER BY season DESC, wins DESC
```

## Team Standings

<DataTable data={team_standings} rows=30 search=true>
  <Column id=team_name title="Team"/>
  <Column id=season title="Season"/>
  <Column id=wins title="W"/>
  <Column id=losses title="L"/>
  <Column id=win_pct title="Win %" fmt="0.0"/>
  <Column id=ppg title="PPG" fmt="0.1"/>
  <Column id=opp_ppg title="Opp PPG" fmt="0.1"/>
  <Column id=point_diff title="+/-" fmt="0.1"/>
</DataTable>

## Points Per Game by Season

```sql team_ppg_trend
SELECT
  season,
  ROUND(AVG(team_pts), 1) as avg_ppg
FROM nba.team_stats
GROUP BY season
ORDER BY season
```

<LineChart
  data={team_ppg_trend}
  x=season
  y=avg_ppg
  title="League Average PPG by Season"
/>
