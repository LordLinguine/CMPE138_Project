--Unoptimized Query 1 (I/O cost: 35.18 MB)
--Player's team name, player full name, player points, and minuted displayed where player scored more than one point, was a starter, 
--and played for more than 30 minutes in a game:
DECLARE var_min_points INT64;
SET var_min_points = 1;
SELECT team_name, full_name, points, minutes
FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE
  points>=var_min_points
  AND starter = TRUE
  AND minutes_int64>30
--Optimized Query 1 (I/O cost: 31.38 MB)
--Player's team name, player full name, player points, and minuted displayed where player scored more than one point, was a starter, 
--and played for more than 30 minutes in a game:
DECLARE var_min_points INT64 DEFAULT 1;
DECLARE var_min_minutes INT64 DEFAULT 30;
WITH minimized_data AS(
  SELECT team_name, full_name, points, minutes_int64
  FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
  WHERE starter = TRUE
    AND minutes_int64 > var_min_minutes
    AND points>=var_min_points
)
SELECT * FROM minimized_data;
--Unoptimized Query 2 (I/O cost: 40.57 MB)
--Player's team name, full name, points, field goal percentage, field goals attempted, and threes attempted displayed
-- for player's who shot more than 50% from the field on 10+ field goal attempts, and 4+ three point attempts
SELECT team_name, full_name, points, field_goals_pct, field_goals_att, three_points_att
FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
WHERE field_goals_pct>50 AND field_goals_att>10 AND three_points_att>4



  
