from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, lit, split, regexp_replace
from pyspark.sql.types import StringType
from additional_data import conference_name, college_mascot, file_extension_to_college, position_name, class_name, college_shorthand_to_name

# Create Spark Session
spark = SparkSession.builder.appName("CFBStatsETL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Set JDBC options for MySQL connection
jdbc_options = {
    "url": spark.conf.get("spark.mysql.cfb_statistics.url"),
    "user": spark.conf.get("spark.mysql.cfb_statistics.user"),
    "password": spark.conf.get("spark.mysql.cfb_statistics.password"),
    "driver": "com.mysql.cj.jdbc.Driver",
}

# Functions to map data from additional_data.py
def map_conference_name(value):
    return conference_name.get(value, None)
map_conference_name_udf = udf(map_conference_name, StringType())

def map_college_mascot(value):
    return college_mascot.get(value, None)
map_college_mascot_udf = udf(map_college_mascot, StringType())

def map_position_name(value):
    return position_name.get(value, None)
map_position_name_udf = udf(map_position_name, StringType())

def map_class_name(value):
    return class_name.get(value, None)
map_class_name_udf = udf(map_class_name, StringType())

def map_college_name(value):
    return college_shorthand_to_name.get(value, None)
map_college_name_udf = udf(map_college_name, StringType())

# Function to remove quotes from player names
def remove_quotes(value):
    return value.strip('"')
remove_quotes_udf = udf(remove_quotes, StringType())

# Function to remove asterisk from playr names
def remove_asterisk(value):
    return value.strip('*')
remove_asterisk_udf = udf(remove_asterisk, StringType())

def remove_comma(value):
    return value.strip(',')
remove_comma_udf = udf(remove_comma, StringType())

# Functions to build dataframes for ETL process
def build_master_college_conf(years):
    for year in years:
        if year == 2024:
            master = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_rushing.csv", header=True, inferSchema=True)
            master = master.withColumn("year", lit(year))
            master = master.select("Team", "Conf", "year")
            master = master.withColumnRenamed("Team", "college_name") \
                .withColumnRenamed("Conf", "conference_shorthand")
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_rushing.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            next_df = next_df.select("Team", "Conf", "year")
            next_df = next_df.withColumnRenamed("Team", "college_name") \
                .withColumnRenamed("Conf", "conference_shorthand")
            master = master.union(next_df)
    return master

def build_master_players(year, teams):
    not_found_list = []
    for year in years:
        years_df = teams.filter(col("year") == year).select("college_name")
        for k, v in file_extension_to_college.items():
            college_df = years_df.filter(col("college_name") == v)
            if college_df.count() > 0:
                if year == 2024 and k == "air_force":
                    master = spark.read.csv(f"/opt/spark/files/in/rosters/{year}_{k}.csv", header=True, inferSchema=True)
                    master = master.select("Player", "Class", "Pos")
                    master = master.withColumn("college_name", lit(v))
                    master = master.withColumn("year", lit(year))
                else:
                    next_df = spark.read.csv(f"/opt/spark/files/in/rosters/{year}_{k}.csv", header=True, inferSchema=True)
                    next_df = next_df.select("Player", "Class", "Pos")
                    next_df = next_df.withColumn("college_name", lit(v))
                    next_df = next_df.withColumn("year", lit(year))
                    master = master.union(next_df)
            else:
                entry = f"{v} not found in {year}"
                not_found_list.append(entry)
    master = master.withColumnRenamed("Player", "player_name") \
        .withColumnRenamed("Class", "class_abbr") \
        .withColumnRenamed("Pos", "position_abbr")
    master = master.withColumn("player_name", when(col("player_name").contains('"'), remove_quotes_udf(col("player_name"))).otherwise(col("player_name")))
    master = master.withColumn("position_abbr", when(col("position_abbr").isNull(), "N/A").otherwise(col("position_abbr")))
    master = master.withColumn("class_abbr", when(col("class_abbr").isNull(), "N/A").otherwise(col("class_abbr")))
    master = master.sort("player_name")
    return master
    
def build_conferences(master):
    conferences = master.select("conference_shorthand").distinct()
    conferences = conferences.sort("conference_shorthand")
    conferences = conferences.withColumn("conference_name", map_conference_name_udf(col("conference_shorthand")))
    return conferences

def build_colleges(master):
    colleges = master.select("college_name")
    colleges = master.select("college_name").distinct()
    colleges = colleges.sort("college_name")
    colleges = colleges.withColumn("college_mascot", map_college_mascot_udf(col("college_name")))
    return colleges

def build_teams(master, colleges, conferences):
    teams = master.select("college_name", "conference_shorthand", "year").distinct()
    teams = teams.join(colleges, "college_name", "inner")
    teams = teams.join(conferences, "conference_shorthand", "inner")
    teams = teams.select("year", "college_id", "conference_id")
    teams = teams.sort( "year", "college_id", "conference_id")
    return teams

def build_positions(master):
    positions = master.select("position_abbr").distinct()
    positions = positions.sort("position_abbr")
    positions = positions.withColumn("position_name", map_position_name_udf(col("position_abbr")))
    return positions

def build_classes(master):
    classes = master.select("class_abbr").distinct()
    classes = classes.sort("class_abbr")
    classes = classes.withColumn("class_name", map_class_name_udf(col("class_abbr")))
    return classes

def build_players(master, positions):
    players = master.select("player_name", "position_abbr").distinct()
    players = players.join(positions, "position_abbr", "inner")
    players = players.select("player_name", "position_id")
    players = players.sort("player_name")
    return players

def build_rosters(master, players, teams, classes):
    rosters = master.select("player_name", "position_abbr", "college_name", "year", "class_abbr")
    rosters = rosters.join(players, on=["player_name", "position_abbr"], how="inner")
    rosters = rosters.join(teams, on=["college_name", "year"], how="inner")
    rosters = rosters.join(classes, on=["class_abbr"], how="inner")
    rosters = rosters.select("player_id", "class_id", "team_id")
    rosters = rosters.sort("team_id", "player_id")
    return rosters

def build_rushing(years,rosters):
    for year in years:
        if year == 2024: 
            rushing = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_rushing.csv", header=True, inferSchema=True)
            rushing = rushing.select(*rushing.columns[1:10])
            rushing = rushing.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_rushing.csv", header=True, inferSchema=True)
            next_df = next_df.select(*next_df.columns[1:10])
            next_df = next_df.withColumn("year", lit(year))
            rushing = rushing.union(next_df)
    rushing = rushing.withColumnRenamed("Player", "player_name") \
                .withColumnRenamed("Team", "college_name") \
                .withColumnRenamed("Conf", "conference_shorthand") \
                .withColumnRenamed("G", "games_played") \
                .withColumnRenamed("Att", "rushing_attempts") \
                .withColumnRenamed("Yds6", "rushing_yards") \
                .withColumnRenamed("Y/A", "rushing_yards_per_attempt") \
                .withColumnRenamed("TD8", "rushing_touchdowns") \
                .withColumnRenamed("Y/G9", "rushing_yards_per_game")     
    rushing = rushing.withColumn("player_name", when(col("player_name").contains('*'), remove_asterisk_udf(col("player_name"))).otherwise(col("player_name")))
    rushing = rushing.withColumn("player_name", when(col("player_name").contains('"'), remove_quotes_udf(col("player_name"))).otherwise(col("player_name")))
    rushing = rushing.join(rosters, on=["player_name", "college_name", "conference_shorthand", "year"], how="inner")
    rushing = rushing.select("roster_id", "games_played", "rushing_attempts", "rushing_yards", "rushing_yards_per_attempt", "rushing_touchdowns", "rushing_yards_per_game")
    rushing = rushing.orderBy("rushing_yards", ascending=False)
    return rushing

def build_receiving(years, rosters):
    for year in years:
        if year == 2024:
            receiving = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_receiving.csv", header=True, inferSchema=True)
            receiving = receiving.select(*receiving.columns[1:10])
            receiving = receiving.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_receiving.csv", header=True, inferSchema=True)
            next_df = next_df.select(*next_df.columns[1:10])
            next_df = next_df.withColumn("year", lit(year))
            receiving = receiving.union(next_df)
    receiving = receiving.withColumnRenamed("Player", "player_name") \
        .withColumnRenamed("Team", "college_name") \
        .withColumnRenamed("Conf", "conference_shorthand") \
        .withColumnRenamed("G", "games_played") \
        .withColumnRenamed("Rec", "receptions") \
        .withColumnRenamed("Yds6", "receiving_yards") \
        .withColumnRenamed("Y/R", "receiving_yards_per_reception") \
        .withColumnRenamed("TD8", "receiving_touchdowns") \
        .withColumnRenamed("Y/G9", "receiving_yards_per_game")
    receiving = receiving.withColumn("player_name", when(col("player_name").contains('*'), remove_asterisk_udf(col("player_name"))).otherwise(col("player_name")))
    receiving = receiving.withColumn("player_name", when(col("player_name").contains('"'), remove_quotes_udf(col("player_name"))).otherwise(col("player_name")))
    receiving = receiving.join(rosters, on=["player_name", "college_name", "conference_shorthand", "year"], how="inner")
    receiving = receiving.select("roster_id", "games_played", "receptions", "receiving_yards", "receiving_yards_per_reception", "receiving_touchdowns", "receiving_yards_per_game")
    receiving = receiving.sort("receiving_yards", ascending=False)
    return receiving

def build_passing(years, rosters):
    for year in years:
        if year == 2024:
            passing = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_passing.csv", header=True, inferSchema=True)
            passing = passing.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_passing.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            passing = passing.union(next_df)
    passing = passing.withColumnRenamed("Player", "player_name") \
        .withColumnRenamed("Team", "college_name") \
        .withColumnRenamed("Conf", "conference_shorthand") \
        .withColumnRenamed("G", "games_played") \
        .withColumnRenamed("Cmp", "completions") \
        .withColumnRenamed("Att", "passing_attempts") \
        .withColumnRenamed("Cmp%", "completion_percentage") \
        .withColumnRenamed("Yds", "passing_yards") \
        .withColumnRenamed("TD", "passing_touchdowns") \
        .withColumnRenamed("TD%", "touchdown_percentage") \
        .withColumnRenamed("Int", "interceptions") \
        .withColumnRenamed("Int%", "interception_percentage") \
        .withColumnRenamed("Y/A", "passing_yards_per_attempt") \
        .withColumnRenamed("Y/C", "passing_yards_per_completion") \
        .withColumnRenamed("Y/G", "passing_yards_per_game") \
        .withColumnRenamed("Rate", "passer_rating")
    passing = passing.withColumn("player_name", when(col("player_name").contains('*'), remove_asterisk_udf(col("player_name"))).otherwise(col("player_name")))
    passing = passing.withColumn("player_name", when(col("player_name").contains('"'), remove_quotes_udf(col("player_name"))).otherwise(col("player_name")))
    passing = passing.withColumn("passing_yards_per_completion", when(col("passing_yards_per_completion").isNull(), 0.0).otherwise(col("passing_yards_per_completion")))
    passing = passing.join(rosters, on=["player_name", "college_name", "conference_shorthand", "year"], how="inner")
    passing = passing.select("roster_id", "games_played", "completions", "passing_attempts", "completion_percentage", "passing_yards", "passing_touchdowns", "touchdown_percentage", "interceptions", "interception_percentage", "passing_yards_per_attempt", "passing_yards_per_completion", "passing_yards_per_game", "passer_rating")
    passing = passing.sort("passing_yards", ascending=False)
    return passing

def build_scoring(years, rosters):
    for year in years:
        if year == 2024:
            scoring = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_scoring.csv", header=True, inferSchema=True)
            scoring = scoring.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_scoring.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            scoring = scoring.union(next_df)
    scoring = scoring.withColumnRenamed("Player", "player_name") \
        .withColumnRenamed("Team", "college_name") \
        .withColumnRenamed("Conf", "conference_shorthand") \
        .withColumnRenamed("G", "games_played") \
        .withColumnRenamed("Rush", "rushing_touchdowns") \
        .withColumnRenamed("Rec", "receiving_touchdowns") \
        .withColumnRenamed("PRTD", "punt_return_touchdowns") \
        .withColumnRenamed("KRTD", "kickoff_return_touchdowns") \
        .withColumnRenamed("FRTD", "fumble_recovery_touchdowns") \
        .withColumnRenamed("IntTD", "interception_return_touchdowns") \
        .withColumnRenamed("OthTD", "other_touchdowns") \
        .withColumnRenamed("TD", "total_touchdowns") \
        .withColumnRenamed("XPM", "extra_points_made") \
        .withColumnRenamed("XPA", "extra_points_attempted") \
        .withColumnRenamed("FGM", "field_goals_made") \
        .withColumnRenamed("FGA", "field_goals_attempted") \
        .withColumnRenamed("2PM", "two_point_conversions_made") \
        .withColumnRenamed("Sfty", "safeties") \
        .withColumnRenamed("Pts", "points_scored") \
        .withColumnRenamed("Pts/G", "points_per_game")
    scoring = scoring.withColumn("player_name", when(col("player_name").contains('*'), remove_asterisk_udf(col("player_name"))).otherwise(col("player_name")))
    scoring = scoring.withColumn("player_name", when(col("player_name").contains('"'), remove_quotes_udf(col("player_name"))).otherwise(col("player_name")))
    scoring = scoring.join(rosters, on=["player_name", "college_name", "conference_shorthand", "year"], how="inner")
    scoring = scoring.select("roster_id", "games_played", "rushing_touchdowns", "receiving_touchdowns", "punt_return_touchdowns", "kickoff_return_touchdowns", "fumble_recovery_touchdowns", "interception_return_touchdowns", "other_touchdowns", "total_touchdowns", "extra_points_made", "extra_points_attempted", "field_goals_made", "field_goals_attempted", "two_point_conversions_made", "safeties", "points_scored", "points_per_game")
    scoring = scoring.sort("points_scored", ascending=False)
    return scoring

def build_punting(years, rosters):
    for year in years:
        if year == 2024:
            punting = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_punting.csv", header=True, inferSchema=True)
            punting = punting.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_punting.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            punting = punting.union(next_df)
    punting = punting.withColumnRenamed("Player", "player_name") \
        .withColumnRenamed("Team", "college_name") \
        .withColumnRenamed("Conf", "conference_shorthand") \
        .withColumnRenamed("G", "games_played") \
        .withColumnRenamed("Pnt", "punts") \
        .withColumnRenamed("Yds", "punt_yards") \
        .withColumnRenamed("Y/P", "yards_per_punt")
    punting = punting.withColumn("player_name", when(col("player_name").contains('*'), remove_asterisk_udf(col("player_name"))).otherwise(col("player_name")))
    punting = punting.withColumn("player_name", when(col("player_name").contains('"'), remove_quotes_udf(col("player_name"))).otherwise(col("player_name")))
    punting = punting.join(rosters, on=["player_name", "college_name", "conference_shorthand", "year"], how="inner")
    punting = punting.select("roster_id", "games_played", "punts", "punt_yards", "yards_per_punt")
    punting = punting.sort("punt_yards", ascending=False)
    return punting

def build_kicking(years, rosters):
    for year in years:
        if year == 2024:
            kicking = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_kicking.csv", header=True, inferSchema=True)
            kicking = kicking.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/ind-stats/{year}_kicking.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            kicking = kicking.union(next_df)
    kicking = kicking.withColumnRenamed("Player", "player_name") \
        .withColumnRenamed("Team", "college_name") \
        .withColumnRenamed("Conf", "conference_shorthand") \
        .withColumnRenamed("G", "games_played") \
        .withColumnRenamed("XPM", "extra_points_made") \
        .withColumnRenamed("XPA", "extra_points_attempted") \
        .withColumnRenamed("XP%", "extra_point_percentage") \
        .withColumnRenamed("FGM", "field_goals_made") \
        .withColumnRenamed("FGA", "field_goals_attempted") \
        .withColumnRenamed("FG%", "field_goal_percentage") \
        .withColumnRenamed("Pts", "Points_scored")
    kicking = kicking.withColumn("player_name", when(col("player_name").contains('*'), remove_asterisk_udf(col("player_name"))).otherwise(col("player_name")))
    kicking = kicking.withColumn("player_name", when(col("player_name").contains('"'), remove_quotes_udf(col("player_name"))).otherwise(col("player_name")))
    kicking = kicking.join(rosters, on=["player_name", "college_name", "conference_shorthand", "year"], how="inner")
    kicking = kicking.select("roster_id", "games_played", "extra_points_made", "extra_points_attempted", "extra_point_percentage", "field_goals_made", "field_goals_attempted", "field_goal_percentage", "points_scored")
    kicking = kicking.withColumn("extra_point_percentage", when(col("extra_point_percentage").isNull(), 0.0).otherwise(col("extra_point_percentage")))
    kicking = kicking.withColumn("field_goal_percentage", when(col("field_goal_percentage").isNull(), 0.0).otherwise(col("field_goal_percentage")))
    kicking = kicking.sort("points_scored", ascending=False)
    return kicking

def build_team_offense(years, teams):
    for year in years:
        if year == 2024:
            team_offense = spark.read.csv(f"/opt/spark/files/in/team-stats/{year}_offense.csv", header=True, inferSchema=True)
            team_offense = team_offense.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/team-stats/{year}_offense.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            team_offense = team_offense.union(next_df)

    team_offense = team_offense.withColumnRenamed("School", "college_name") \
        .withColumnRenamed("G", "games_played") \
        .withColumnRenamed("Pts", "points_per_game") \
        .withColumnRenamed("Cmp", "completions_per_game") \
        .withColumnRenamed("Att5", "passing_attempts_per_game") \
        .withColumnRenamed("Pct", "completion_percentage_per_game") \
        .withColumnRenamed("Yds7", "passing_yards_per_game") \
        .withColumnRenamed("TD8", "passing_touchdowns_per_game") \
        .withColumnRenamed("Att9", "rushing_attempts_per_game") \
        .withColumnRenamed("Yds10", "rushing_yards_per_game") \
        .withColumnRenamed("Avg11", "rushing_yards_per_attempt_per_game") \
        .withColumnRenamed("TD12", "rushing_touchdowns_per_game") \
        .withColumnRenamed("Plays", "total_plays_per_game") \
        .withColumnRenamed("Yds14", "total_yards_per_game") \
        .withColumnRenamed("Avg15", "yards_per_play_per_game") \
        .withColumnRenamed("Pass", "passing_first_downs_per_game") \
        .withColumnRenamed("Rush", "rushing_first_downs_per_game") \
        .withColumnRenamed("Pen", "penalty_first_downs_per_game") \
        .withColumnRenamed("Tot19", "total_first_downs_per_game") \
        .withColumnRenamed("No.", "penalties_per_game") \
        .withColumnRenamed("Yds21", "penalty_yards_per_game") \
        .withColumnRenamed("Fum", "fumbles_lost_per_game") \
        .withColumnRenamed("Int", "interceptions_per_game") \
        .withColumnRenamed("Tot24", "turnovers_per_game")
    team_offense = team_offense.withColumn("college_name", when(col("college_name").isin(list(college_shorthand_to_name.keys())), map_college_name_udf(col("college_name"))).otherwise(col("college_name")))
    team_offense = team_offense.join(teams, on=["college_name", "year"], how="inner")
    team_offense = team_offense.select("team_id", "games_played", "points_per_game", "completions_per_game", "passing_attempts_per_game", "completion_percentage_per_game", "passing_yards_per_game", "passing_touchdowns_per_game", "rushing_attempts_per_game", "rushing_yards_per_game", "rushing_yards_per_attempt_per_game", "rushing_touchdowns_per_game", "total_plays_per_game", "total_yards_per_game", "yards_per_play_per_game", "passing_first_downs_per_game", "rushing_first_downs_per_game", "penalty_first_downs_per_game", "total_first_downs_per_game", "penalties_per_game", "penalty_yards_per_game", "fumbles_lost_per_game", "interceptions_per_game", "turnovers_per_game")
    team_offense = team_offense.sort("points_per_game", ascending=False)
    return team_offense

def build_team_defense(years, teams):
    for year in years:
        if year == 2024:
            team_defense = spark.read.csv(f"/opt/spark/files/in/team-stats/{year}_defense.csv", header=True, inferSchema=True)
            team_defense = team_defense.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/team-stats/{year}_defense.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            team_defense = team_defense.union(next_df)
    team_defense = team_defense.withColumnRenamed("School", "college_name") \
        .withColumnRenamed("G", "games_played") \
        .withColumnRenamed("Pts", "opponent_points_per_game") \
        .withColumnRenamed("Cmp", "opponent_completions_per_game") \
        .withColumnRenamed("Att5", "opponent_passing_attempts_per_game") \
        .withColumnRenamed("Pct", "opponent_completion_percentage_per_game") \
        .withColumnRenamed("Yds7", "opponent_passing_yards_per_game") \
        .withColumnRenamed("TD8", "opponent_passing_touchdowns_per_game") \
        .withColumnRenamed("Att9", "opponent_rushing_attempts_per_game") \
        .withColumnRenamed("Yds10", "opponent_rushing_yards_per_game") \
        .withColumnRenamed("Avg11", "opponent_rushing_yards_per_attempt_per_game") \
        .withColumnRenamed("TD12", "opponent_rushing_touchdowns_per_game") \
        .withColumnRenamed("Plays", "opponent_total_plays_per_game") \
        .withColumnRenamed("Yds14", "opponent_total_yards_per_game") \
        .withColumnRenamed("Avg15", "opponent_yards_per_play_per_game") \
        .withColumnRenamed("Pass", "opponent_passing_first_downs_per_game") \
        .withColumnRenamed("Rush", "opponent_rushing_first_downs_per_game") \
        .withColumnRenamed("Pen", "opponent_penalty_first_downs_per_game") \
        .withColumnRenamed("Tot", "opponent_total_first_downs_per_game") \
        .withColumnRenamed("No.", "opponent_penalties_per_game") \
        .withColumnRenamed("Yds21", "opponent_penalty_yards_per_game") \
        .withColumnRenamed("Fum", "opponent_fumbles_lost_per_game") \
        .withColumnRenamed("Int", "opponent_interceptions_per_game") \
        .withColumnRenamed("TO", "opponent_turnovers_per_game")
    team_defense = team_defense.withColumn("college_name", when(col("college_name").isin(list(college_shorthand_to_name.keys())), map_college_name_udf(col("college_name"))).otherwise(col("college_name")))
    team_defense = team_defense.join(teams, on=["college_name", "year"], how="inner")
    team_defense = team_defense.select("team_id", "games_played", "opponent_points_per_game", "opponent_completions_per_game", "opponent_passing_attempts_per_game", "opponent_completion_percentage_per_game", "opponent_passing_yards_per_game", "opponent_passing_touchdowns_per_game", "opponent_rushing_attempts_per_game", "opponent_rushing_yards_per_game", "opponent_rushing_yards_per_attempt_per_game", "opponent_rushing_touchdowns_per_game", "opponent_total_plays_per_game", "opponent_total_yards_per_game", "opponent_yards_per_play_per_game", "opponent_passing_first_downs_per_game", "opponent_rushing_first_downs_per_game", "opponent_penalty_first_downs_per_game", "opponent_total_first_downs_per_game", "opponent_penalties_per_game", "opponent_penalty_yards_per_game", "opponent_fumbles_lost_per_game", "opponent_interceptions_per_game", "opponent_turnovers_per_game")
    team_defense = team_defense.sort("opponent_points_per_game", ascending=True)
    return team_defense  

def build_team_special(years, teams):
    for year in years:
        if year == 2024:
            team_special = spark.read.csv(f"/opt/spark/files/in/team-stats/{year}_special_teams.csv", header=True, inferSchema=True)
            team_special = team_special.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/team-stats/{year}_special_teams.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            team_special = team_special.union(next_df)
    team_special = team_special.withColumnRenamed("School", "college_name") \
        .withColumnRenamed("G", "games_played") \
        .withColumnRenamed("XPM", "extra_points_made_per_game") \
        .withColumnRenamed("XPA", "extra_points_attempted_per_game") \
        .withColumnRenamed("XP%", "extra_point_percentage") \
        .withColumnRenamed("FGM", "field_goals_made_per_game") \
        .withColumnRenamed("FGA", "field_goals_attempted_per_game") \
        .withColumnRenamed("FG%", "field_goal_percentage") \
        .withColumnRenamed("Pts", "kicking_points_scored_per_game") \
        .withColumnRenamed("Punts", "punts_per_game") \
        .withColumnRenamed("Yds11", "punt_yards_per_game") \
        .withColumnRenamed("Avg12", "yards_per_punt_per_game") \
        .withColumnRenamed("Ret13", "kickoff_returns_per_game") \
        .withColumnRenamed("Yds14", "kickoff_return_yards_per_game") \
        .withColumnRenamed("Avg15", "return_yards_per_kickoff_per_game") \
        .withColumnRenamed("TD16", "kickoff_return_touchdowns_per_game") \
        .withColumnRenamed("Ret17", "punt_returns_per_game") \
        .withColumnRenamed("Yds18", "punt_return_yards_per_game") \
        .withColumnRenamed("Avg19", "return_yards_per_punt_per_game") \
        .withColumnRenamed("TD20", "punt_return_touchdowns_per_game")
    team_special = team_special.withColumn("college_name", when(col("college_name").isin(list(college_shorthand_to_name.keys())), map_college_name_udf(col("college_name"))).otherwise(col("college_name")))
    team_special = team_special.join(teams, on=["college_name", "year"], how="inner")
    team_special = team_special.select("team_id", "games_played", "extra_points_made_per_game", "extra_points_attempted_per_game", "extra_point_percentage", "field_goals_made_per_game", "field_goals_attempted_per_game", "field_goal_percentage", "kicking_points_scored_per_game", "punts_per_game", "punt_yards_per_game", "yards_per_punt_per_game", "kickoff_returns_per_game", "kickoff_return_yards_per_game", "return_yards_per_kickoff_per_game", "kickoff_return_touchdowns_per_game", "punt_returns_per_game", "punt_return_yards_per_game", "return_yards_per_punt_per_game", "punt_return_touchdowns_per_game")
    team_special = team_special.sort("team_id", ascending=True)
        
    return team_special

def build_standings(years, teams):
    for year in years:
        if year == 2024:
            standings = spark.read.csv(f"/opt/spark/files/in/standings/{year}_standings.csv", header=True, inferSchema=True)
            standings = standings.withColumn("year", lit(year))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/standings/{year}_standings.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("year", lit(year))
            standings = standings.union(next_df)
    standings = standings.withColumnRenamed("School", "college_name") \
        .withColumnRenamed("Conf", "conference_shorthand") \
        .withColumnRenamed("W3", "total_wins") \
        .withColumnRenamed("L4", "total_losses") \
        .withColumnRenamed("Pct5", "total_win_percentage") \
        .withColumnRenamed("W6", "conference_wins") \
        .withColumnRenamed("L7", "conference_losses") \
        .withColumnRenamed("Pct8", "conference_win_percentage") \
        .withColumnRenamed("Off", "points_scored_per_game") \
        .withColumnRenamed("Def", "points_allowed_per_game") \
        .withColumnRenamed("SRS", "simple_rating_system") \
        .withColumnRenamed("SOS", "strength_of_schedule") \
        .withColumnRenamed("AP Pre", "ap_preseason_rank") \
        .withColumnRenamed("AP High", "ap_highest_rank") \
        .withColumnRenamed("AP Post", "ap_final_rank") 
    standings = standings.withColumn("conference_shorthand", regexp_replace("conference_shorthand", r"\s*\(.*?\)", ""))
    standings = standings.withColumn("conference_wins", when(col("conference_wins").isNull(), 0).otherwise(col("conference_wins")))
    standings = standings.withColumn("conference_losses", when(col("conference_losses").isNull(), 0).otherwise(col("conference_losses")))
    standings = standings.withColumn("conference_win_percentage", when(col("conference_win_percentage").isNull(), 0.0).otherwise(col("conference_win_percentage")))
    standings = standings.join(teams, on=["college_name", "conference_shorthand", "year"], how="inner")
    standings = standings.select("team_id", "total_wins", "total_losses", "total_win_percentage", "conference_wins", "conference_losses", "conference_win_percentage", "points_scored_per_game", "points_allowed_per_game", "simple_rating_system", "strength_of_schedule", "ap_preseason_rank", "ap_highest_rank", "ap_final_rank")
    return standings

# List of years to process
years = [2024, 2023, 2022, 2021, 2020]

# Create master dataframe for college and conference
master_college_conf = build_master_college_conf(years)

# ETL process for conference table
conferences = build_conferences(master_college_conf)
conferences.write.format("jdbc").options(**jdbc_options, dbtable="conference").mode("append").save()

# ETL Process for college table
colleges = build_colleges(master_college_conf)
colleges.write.format("jdbc").options(**jdbc_options, dbtable="college").mode("append").save()

# ETL Process for team table
# - Read college and conference tables
conferences = spark.read.format("jdbc").options(**jdbc_options, dbtable="conference").load()
colleges = spark.read.format("jdbc").options(**jdbc_options, dbtable="college").load()
# - Build and write teams dataframe
teams = build_teams(master_college_conf, colleges, conferences)
teams.write.format("jdbc").options(**jdbc_options, dbtable="team").mode("append").save()

# Create master dataframe for players
# - Read team table join with college and conference tables
teams = spark.read.format("jdbc").options(**jdbc_options, dbtable="team").load()
teams = teams.join(colleges, "college_id", "inner")
teams = teams.join(conferences, "conference_id", "inner")
# - Build master players dataframe for future ETL processes
master_players = build_master_players(years, teams)

# ETL Process for position table
positions = build_positions(master_players)
positions.write.format("jdbc").options(**jdbc_options, dbtable="`position`").mode("append").save()

# ETL Process for class table
classes = build_classes(master_players)
classes.write.format("jdbc").options(**jdbc_options, dbtable="class").mode("append").save()

# ETL Process for player table
# - Read position table
positions = spark.read.format("jdbc").options(**jdbc_options, dbtable="`position`").load()
# - Build and write players dataframe
players = build_players(master_players, positions)
players.write.format("jdbc").options(**jdbc_options, dbtable="player").mode("append").save()

# ETL Process for roster table
# - Read player table, join with position table
players = spark.read.format("jdbc").options(**jdbc_options, dbtable="player").load()
players = players.join(positions, "position_id", "inner")
# - Read class table
classes = spark.read.format("jdbc").options(**jdbc_options, dbtable="class").load()
# - Build and write roster dataframe
rosters = build_rosters(master_players, players, teams, classes)
rosters.write.format("jdbc").options(**jdbc_options, dbtable="roster").mode("append").save()

# ETL Process for rushing table
# - Read rosters from SQL join with players, teams, classes
rosters = spark.read.format("jdbc").options(**jdbc_options, dbtable="roster").load()
rosters = rosters.join(players, "player_id", "inner")
rosters = rosters.join(teams, "team_id", "inner")
rosters = rosters.join(classes, "class_id", "inner")
# - Build and write rushing dataframe
rushing = build_rushing(years, rosters)
rushing.write.format("jdbc").options(**jdbc_options, dbtable="rushing_stat").mode("append").save()

# ETL Process for receiving table
receiving = build_receiving(years, rosters)
receiving.write.format("jdbc").options(**jdbc_options, dbtable="receiving_stat").mode("append").save()

# ETL process for passing table
passing = build_passing(years, rosters)
passing.write.format("jdbc").options(**jdbc_options, dbtable="passing_stat").mode("append").save()

# ETL process for scoring table
scoring = build_scoring(years, rosters)
scoring.write.format("jdbc").options(**jdbc_options, dbtable="scoring_stat").mode("append").save()

# ETL process for punting table
punting = build_punting(years, rosters)
punting.write.format("jdbc").options(**jdbc_options, dbtable="punting_stat").mode("append").save()

# ETL process for kicking table
kicking = build_kicking(years, rosters)
kicking.write.format("jdbc").options(**jdbc_options, dbtable="kicking_stat").mode("append").save()

# ETL process for team offense table
team_offense = build_team_offense(years, teams)
team_offense.write.format("jdbc").options(**jdbc_options, dbtable="team_offense").mode("append").save()

# ETL process for team defense table
team_defense = build_team_defense(years, teams)
team_defense.write.format("jdbc").options(**jdbc_options, dbtable="team_defense").mode("append").save()

# ETL process for team special table
team_special = build_team_special(years, teams)
team_special.write.format("jdbc").options(**jdbc_options, dbtable="team_special").mode("append").save()

#ETL process for standings table
standings = build_standings(years, teams)
standings.write.format("jdbc").options(**jdbc_options, dbtable="team_standing").mode("append").save()

spark.stop()