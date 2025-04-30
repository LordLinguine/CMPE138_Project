
-- Players who improved their scoring between 2 seasons
-- (56.55 mb)
WITH player_season_stats AS (
  SELECT 
    player_id,
    full_name,
    season,
    AVG(points) AS avg_points
  FROM 
    `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
  WHERE
  points IS NOT NULL
  GROUP BY 
    player_id, full_name, season
)
SELECT 
  a.full_name,
  a.season AS season1,
  b.season AS season2,
  a.avg_points AS avg_points_season1,
  b.avg_points AS avg_points_season2,
  (b.avg_points - a.avg_points) AS improvement
FROM 
  player_season_stats a
JOIN 
  player_season_stats b
ON 
  a.player_id = b.player_id AND a.season = b.season - 1
WHERE 
  (b.avg_points - a.avg_points) > 5
ORDER BY 
  improvement DESC
LIMIT 15;

-- Top players by 3 point shooting percentage, at least 20 attempts needed
-- (22.41 MB)
SELECT 
  full_name,
  three_points_pct,
  three_points_att
FROM 
  `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE 
  three_points_att > 20 AND three_points_pct IS NOT NULL
ORDER BY 
  three_points_pct DESC
LIMIT 10;

-- Top Free Throw Shooters, minimum 30 attempts (Better Optimized)
-- (22.37 MB)
SELECT 
  full_name,
  SUM(free_throws_made) AS total_made,
  SUM(free_throws_att) AS total_attempted,
  SAFE_DIVIDE(SUM(free_throws_made), SUM(free_throws_att)) AS free_throw_pct
FROM 
  `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE 
  free_throws_att IS NOT NULL AND free_throws_att IS NOT NULL
GROUP BY 
  full_name
HAVING 
  total_attempted >= 30
ORDER BY 
  free_throw_pct DESC
LIMIT 10;

-- Top Free Throw Shooters, minimum 30 attempts (Less Optimized)
-- Adds extra subquery to check that free throw attempts are more than 0
-- (54.58 MB)
SELECT 
  full_name,
  SUM(free_throws_made) AS total_made,
  SUM(free_throws_att) AS total_attempted,
  SAFE_DIVIDE(SUM(free_throws_made), SUM(free_throws_att)) AS free_throw_pct
FROM 
  `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE 
  player_id IN (
    SELECT player_id
    FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
    WHERE free_throws_att > 0  -- This subquery returns a large dataset
  )
GROUP BY 
  full_name
HAVING 
  total_attempted >= 30
ORDER BY 
  free_throw_pct DESC
LIMIT 10;
