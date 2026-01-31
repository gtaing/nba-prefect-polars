---
title: NBA Season Statistics
---

# NBA Season Statistics Dashboard

<LastRefreshed/>

```sql team_summary
SELECT
  COUNT(DISTINCT team) as total_teams,
  COUNT(DISTINCT season) as total_seasons,
  MAX(season) as latest_season
FROM nba.team_stats
```

<Grid cols=3>
  <BigValue
    data={team_summary}
    value=total_teams
    title="Teams Tracked"
  />
  <BigValue
    data={team_summary}
    value=total_seasons
    title="Seasons"
  />
  <BigValue
    data={team_summary}
    value=latest_season
    title="Latest Season"
  />
</Grid>

## Navigation

- [Team Statistics](/teams) - View team performance by season
- [Player Statistics](/players) - View player performance metrics
