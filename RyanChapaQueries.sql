--Clutch stats
-- Query 1 (unoptimized): finds the players who are the most reliable clutch three-point shooters in the final five minutes of the second half.
WITH
  clutch_shots AS (
  SELECT
    player_full_name,
    shot_made,
    game_id,
    period
  FROM
    bigquery-public-data.ncaa_basketball.mbb_pbp_sr
  WHERE
    period = 2
    AND SAFE_CAST(SPLIT(game_clock, ':')[
    OFFSET
      (0)] AS INT64) * 60 + SAFE_CAST(SPLIT(game_clock, ':')[
    OFFSET
      (1)] AS INT64) <= 300
    AND three_point_shot = TRUE ),
  player_stats AS (
  SELECT
    player_full_name,
    COUNT(*) AS clutch_3pa,
    COUNTIF(shot_made = TRUE) AS clutch_3pm
  FROM
    clutch_shots
  GROUP BY
    player_full_name )
SELECT
  player_full_name,
  clutch_3pm,
  clutch_3pa,
  ROUND(SAFE_DIVIDE(clutch_3pm, clutch_3pa), 3) AS clutch_3pt_pct
FROM
  player_stats
WHERE
  clutch_3pa >= 10
  AND SAFE_DIVIDE(clutch_3pm, clutch_3pa) >= 0.4

--Top 25 Players
--Query 2 (unoptimized): Identifies the top 25 players with the best custom performance score including points, rebounds, assists, and steals.
SELECT
  full_name,
  team_name,
  position,
  points,
  rebounds,
  assists,
  steals,
  ROUND(points + rebounds * 1.2 + assists * 1.5 + steals * 2, 1) AS performance_score
FROM
  `bigquery-public-data.ncaa_basketball.mbb_player_stats`
WHERE
  points IS NOT NULL
ORDER BY
  performance_score DESC
LIMIT 25

--Top 10 Rebound
-- Query 3 (unoptimized): top 10 players ranked by their average rebounds per game, 
--but only for those who have pulled down at least 100 total rebounds.
--LESS OPTIMIZED (49.8 MB)

SELECT
  full_name,
  SUM(rebounds) AS total_rebounds,
  COUNT(*) AS games_played,
  ROUND(SAFE_DIVIDE(SUM(rebounds), COUNT(*)), 2) AS rebounds_per_game
FROM
  `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE
  player_id IN (
    SELECT player_id
    FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
    WHERE rebounds > 0
  )
GROUP BY
  full_name
HAVING
  total_rebounds >= 100
ORDER BY
  rebounds_per_game DESC
LIMIT 10;


-- Top 10 Rebound
-- Query 3 (optimized): top 10 players ranked by their average rebounds per game, 
--but only for those who have pulled down at least 100 total rebounds.
--OPTIMIZED (18.43 MB)
SELECT
  full_name,
  SUM(rebounds)            AS total_rebounds,
  COUNT(*)                 AS games_played,
  ROUND(
    SAFE_DIVIDE(SUM(rebounds), COUNT(*))
  , 2)                      AS rebounds_per_game
FROM
  `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE
  played = TRUE            -- drop bench rows entirely
  AND rebounds IS NOT NULL -- skip nulls up front
GROUP BY
  full_name
HAVING
  total_rebounds >= 100
ORDER BY
  rebounds_per_game DESC
LIMIT 10;

