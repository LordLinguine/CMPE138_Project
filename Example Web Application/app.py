from flask import Flask, render_template, request, jsonify
from google.cloud import bigquery
import os
import traceback
import pandas as pd

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bq-key.json"

app = Flask(__name__)

try:
    client = bigquery.Client()
    print("BigQuery client initialized successfully")
except Exception as e:
    print(f"Error initializing BigQuery client: {e}")
    client = None

@app.route("/", methods=["GET"])
def home():
    try:
        teams_query = """
            SELECT DISTINCT team_name
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            ORDER BY team_name
        """
        seasons_query = """
            SELECT DISTINCT season
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            ORDER BY season DESC
        """
        teams_df = client.query(teams_query).to_dataframe()
        seasons_df = client.query(seasons_query).to_dataframe()

        teams = teams_df["team_name"].dropna().tolist()
        seasons = seasons_df["season"].dropna().tolist()

        return render_template("index.html", teams=teams, available_seasons=seasons, selected_team=None, selected_season=None, results=None, season_note="Choose a season to see data.")
    except Exception as e:
        return f"Error loading home page: {str(e)}"
    

@app.route("/get_stats", methods=["POST"])
def get_stats():
    team = request.form["team"]
    season = request.form["season"]

    try:
        teams_query = """
            SELECT DISTINCT team_name
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            ORDER BY team_name
        """
        seasons_query = """
            SELECT DISTINCT season
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            ORDER BY season DESC
        """
        teams_df = client.query(teams_query).to_dataframe()
        seasons_df = client.query(seasons_query).to_dataframe()

        teams = teams_df["team_name"].dropna().tolist()
        seasons = seasons_df["season"].dropna().tolist()
        
        query = f"""
            SELECT full_name, points, assists, field_goals_pct, three_points_pct
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            WHERE team_name = '{team}' AND season = {season}
            ORDER BY points DESC
            LIMIT 10
        """
        df = client.query(query).to_dataframe()

        return render_template("index.html", teams=teams, available_seasons=seasons, selected_team=team, selected_season=season, results=df.to_dict(orient="records"), season_note=f"Stats for {team} in {season} season.")
    except Exception as e:
        return f"Error fetching stats: {str(e)}"
    
@app.route("/search_player", methods=["POST"])
def search_player():
    player_name = request.form["player_name"]
    season = request.form.get("season", "")  # Optional season filter
    
    try:
        teams_query = """
            SELECT DISTINCT team_name
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            ORDER BY team_name
        """
        seasons_query = """
            SELECT DISTINCT season
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            ORDER BY season DESC
        """
        teams_df = client.query(teams_query).to_dataframe()
        seasons_df = client.query(seasons_query).to_dataframe()

        teams = teams_df["team_name"].dropna().tolist()
        available_seasons = seasons_df["season"].dropna().tolist()
        
        # Build the query with a LIKE operator for partial name matching
        season_filter = f"AND season = {season}" if season else ""
        query = f"""
            SELECT 
                full_name, 
                team_name, 
                season,
                AVG(points) as avg_points, 
                AVG(assists) as avg_assists, 
                AVG(field_goals_pct) as avg_fg_pct, 
                AVG(three_points_pct) as avg_3pt_pct,
                COUNT(*) as games_played
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            WHERE LOWER(full_name) LIKE LOWER('%{player_name}%') {season_filter}
            GROUP BY full_name, team_name, season
            ORDER BY avg_points DESC
            LIMIT 10
        """
        
        df = client.query(query).to_dataframe()
        
        # Format percentages for display
        if not df.empty:
            # if 'avg_fg_pct' in df.columns:
            #     df['avg_fg_pct'] = df['avg_fg_pct'] * 100
            # if 'avg_3pt_pct' in df.columns:
            #     df['avg_3pt_pct'] = df['avg_3pt_pct'] * 100
            # Round average statistics
            if 'avg_points' in df.columns:
                df['avg_points'] = df['avg_points'].round(1)
            if 'avg_assists' in df.columns:
                df['avg_assists'] = df['avg_assists'].round(1)

        return render_template(
            "index.html", 
            teams=teams, 
            available_seasons=available_seasons,
            selected_team=None, 
            selected_season=season if season else None, 
            player_results=df.to_dict(orient="records") if not df.empty else None,
            player_search=player_name,
            search_note=f"Search results for player: '{player_name}'" + (f" in season {season}" if season else "")
        )
    except Exception as e:
        return f"Error searching for player: {str(e)}"

@app.route("/min-points")
def min_points():
    try:
        query = """
            DECLARE var_min_points INT64; 
            SET var_min_points = 1; 
            SELECT team_name, full_name, points, assists, minutes_int64
            FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            WHERE points >= var_min_points
            LIMIT 100
        """
        df = client.query(query).to_dataframe()
        return render_template("min_points.html", results=df.to_dict(orient="records"))
    except Exception as e:
        return f"Error fetching data: {str(e)}"

@app.route("/high-efficiency")
def high_efficiency():
    try:
        query = """
            WITH HighEfficiencyPlayers AS (
              SELECT
                team_name, full_name, points, field_goals_pct, field_goals_made, field_goals_att, three_points_made, three_points_att
              FROM `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
              WHERE
                field_goals_pct > 0.5
                AND field_goals_att > 10
                AND three_points_att > 4
            )
            SELECT points, team_name, full_name, field_goals_pct
            FROM HighEfficiencyPlayers
            ORDER BY field_goals_pct DESC
            LIMIT 10
        """
        df = client.query(query).to_dataframe()
        # Convert field_goals_pct from decimal to percentage for display
        #df['field_goals_pct'] = df['field_goals_pct'] * 100
        return render_template("high_efficiency.html", results=df.to_dict(orient="records"))
    except Exception as e:
        return f"Error fetching data: {str(e)}"

@app.route("/improved-scoring")
def improved_scoring():
    try:
        query = """
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
            LIMIT 15
        """
        df = client.query(query).to_dataframe()
        # Round floating point numbers for better display
        df['avg_points_season1'] = df['avg_points_season1'].round(1)
        df['avg_points_season2'] = df['avg_points_season2'].round(1)
        df['improvement'] = df['improvement'].round(1)
        return render_template("improved_scoring.html", results=df.to_dict(orient="records"))
    except Exception as e:
        return f"Error fetching data: {str(e)}"

@app.route("/top-3pt-shooters")
def top_three_point():
    try:
        query = """
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
            LIMIT 10
        """
        df = client.query(query).to_dataframe()
        # Convert three_points_pct from decimal to percentage for display
        # Just multiply by 100 but keep it as a numeric value
       # df['three_points_pct'] = df['three_points_pct'] * 100
        return render_template("top_three_point.html", results=df.to_dict(orient="records"))
    except Exception as e:
        return f"Error fetching data: {str(e)}"

@app.route("/top-free-throws")
def top_free_throws():
    try:
        query = """
            SELECT 
              full_name,
              SUM(free_throws_made) AS total_made,
              SUM(free_throws_att) AS total_attempted,
              SAFE_DIVIDE(SUM(free_throws_made), SUM(free_throws_att)) AS free_throw_pct
            FROM 
              `bigquery-public-data.ncaa_basketball.mbb_players_games_sr`
            WHERE 
              free_throws_att IS NOT NULL
            GROUP BY 
              full_name
            HAVING 
              total_attempted >= 30
            ORDER BY 
              free_throw_pct DESC
            LIMIT 10
        """
        df = client.query(query).to_dataframe()
        # Convert free_throw_pct from decimal to percentage for display
        df['free_throw_pct'] = df['free_throw_pct'] * 100
        return render_template("top_free_throws.html", results=df.to_dict(orient="records"))
    except Exception as e:
        return f"Error fetching data: {str(e)}"

if __name__ == "__main__":
    app.run(debug=True)

