--Unoptimized Query 1 (I/O cost: 67.39 MB)
--Player's team name, player full name, player points, and minutes displayed where player scored more than one point, was a starter, 
--and played for more than 30 minutes in a game:
--ordered by full name
DECLARE var_min_points INT64;
SET var_min_points = 1;
SELECT a.team_name, a.full_name, a.points, a.minutes
FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr` AS a
JOIN `bigquery-public-data.ncaa_basketball.mbb_players_games_sr` AS b
  ON a.full_name = b.full_name
  AND a.game_id = b.game_id
WHERE
  a.points >= var_min_points
  AND a.starter = TRUE
  AND a.minutes_int64 > 30
GROUP BY
  a.team_name, a.full_name, a.points, a.minutes
ORDER BY
  a.full_name
--Optimized Query 1 (I/O cost: 35.18 MB)
--Player's team name, player full name, player points, and minutes displayed where player scored more than one point, was a starter, 
--and played for more than 30 minutes in a game:
--ordered by full name
DECLARE var_min_points INT64;
SET var_min_points = 1;
WITH filtered_out_data AS (
  SELECT team_name, full_name, points, minutes
  FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
  WHERE
    points >= var_min_points
    AND starter = TRUE
    AND minutes_int64 > 30
  )
  SELECT f.team_name, f.full_name, f.points, f.minutes
  FROM filtered_out_data f
  LEFT JOIN UNNEST([1]) AS j ON TRUE
  GROUP BY f.team_name, f.full_name, f.points, f.minutes
  ORDER BY f.full_name
--Unoptimized/Optimized Query 2 (I/O cost: 72.78 MB)
--Player's team name, full name, points, field goal percentage, field goals attempted, and threes attempted displayed
-- for player's who shot more than 50% from the field on 10+ field goal attempts, and 4+ three point attempts
--ordered by full name
SELECT a.team_name, a.full_name, a.points, a.field_goal_pct, a.field_goals_att, a.three_points_att
FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr` AS a
JOIN `bigquery-public-data.ncaa_basketball.mbb_players_games_sr` AS b
  ON a.full_name = b.full_name AND a.game_id = b.game_id
WHERE a.field_goals_pct > 50, AND a.field_goals_att > 10, AND a.three_points_att > 4
GROUP BY a.team_name, a.full_name, a.points, a.field_goals_pct, a.field_goals_att, a.three_points_att
ORDER BY a.full_name;


  
