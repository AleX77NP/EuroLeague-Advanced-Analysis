WITH starter AS (
  SELECT 
    team, 
    win, 
    season, 
    game_no 
  FROM 
    euroleague_games
), 
lagged AS (
  SELECT 
    *, 
    LAG(win, 1) OVER (
      PARTITION BY team, 
      season 
      ORDER BY 
        game_no
    ) AS win_previous 
  FROM 
    starter
),
 streak_change AS (
  SELECT 
    *,
    CASE WHEN win <> win_previous THEN 1 ELSE 0 END AS streak_changed 
  FROM 
    lagged
), 
streak_identified AS (
  SELECT 
    *, 
    SUM(streak_changed) OVER (
      PARTITION BY team, 
      season 
      ORDER BY 
        game_no
    ) AS streak_identifier 
  FROM 
    streak_change
), 
record_counts AS (
  SELECT 
    *, 
    ROW_NUMBER() OVER (
      PARTITION BY team, 
      season, 
      streak_identifier 
      ORDER BY 
        game_no
    ) AS streak_length 
  FROM 
    streak_identified
), 
ranked AS (
  SELECT 
    *, 
    RANK() OVER (
      PARTITION BY team, 
      season, 
      streak_identifier 
      ORDER BY 
        streak_length DESC
    ) AS rank 
  FROM 
    record_counts
)

SELECT 
  team,
  season,
  streak_length
FROM 
  ranked 
WHERE 
  rank = 1 
ORDER BY 
  streak_length DESC
