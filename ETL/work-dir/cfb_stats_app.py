from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, lit, split
from pyspark.sql.types import StringType
from additional_data import conference_name, college_mascot, file_extension_to_college, position_name, class_name

# Create Spark Session
spark = SparkSession.builder.appName("CFBStatsETL").getOrCreate()

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

# Function to remove quotes from player names
def remove_quotes(value):
    return value.strip('"')
remove_quotes_udf = udf(remove_quotes, StringType())

# Function to remove asterisk from playr names
def remove_asterisk(value):
    return value.strip('*')
remove_asterisk_udf = udf(remove_asterisk, StringType())

# Function to aggregate player dataframe
def create_player_dataframe(year):
    for k, v in file_extension_to_college.items():
        if k == "air_force":
            master_players = spark.read.csv(f"/opt/spark/files/in/rosters/{year}_{k}.csv", header=True, inferSchema=True)
            master_players = master_players.withColumn("college_name", lit(v))
            master_players = master_players.withColumn("year", lit(year))
            master_players = master_players.drop(col("Summary"))
        else:
            next_df = spark.read.csv(f"/opt/spark/files/in/rosters/{year}_{k}.csv", header=True, inferSchema=True)
            next_df = next_df.withColumn("college_name", lit(v))
            next_df = next_df.withColumn("year", lit(year))
            next_df = next_df.drop(col("Summary"))
            master_players = master_players.union(next_df)
    return master_players


# Create master dataframe from rushing stats
# Used to populate conference and college tables
master_college_conf = spark.read.csv("/opt/spark/files/in/ind-stats/2024_rushing.csv", header=True, inferSchema=True)

# ETL process for conference table
##conferences = master_college_conf.select("Conf").distinct()
##pac12 = spark.createDataFrame([("Pac-12",)], ["Conf"])
##conferences = conferences.union(pac12)
##conferences = conferences.sort("Conf")
##conferences = conferences.withColumn("conference_name", map_conference_name_udf(col("Conf")))
##conferences = conferences.withColumnRenamed("Conf", "conference_shorthand")
##conferences.write.format("jdbc").options(**jdbc_options, dbtable="conference").mode("append").save()

# ETL Process for college table
##colleges = master_college_conf.select("Team").distinct()
##colleges = colleges.sort("Team")
##colleges = colleges.withColumn("college_mascot", map_college_mascot_udf(col("Team")))
##colleges = colleges.withColumnRenamed("Team", "college_name")
##colleges.write.format("jdbc").options(**jdbc_options, dbtable="college").mode("append").save()

# ETL Process for team table
##teams = master_college_conf.select("Team", "Conf").distinct()
##teams = teams.withColumnRenamed("Team", "college_name")
##teams = teams.withColumnRenamed("Conf", "conference_shorthand")
colleges = spark.read.format("jdbc").options(**jdbc_options, dbtable="college").load()
conferences = spark.read.format("jdbc").options(**jdbc_options, dbtable="conference").load()
##teams = teams.join(colleges, "college_name", "inner")
##teams = teams.join(conferences, "conference_shorthand", "inner")
##teams = teams.withColumn("year", lit(2024))
##teams = teams.select("year", "college_id", "conference_id")
##teams = teams.sort("year", "college_id", "conference_id")
##teams.write.format("jdbc").options(**jdbc_options, dbtable="team").mode("append").save()

# Create master player dataframe
# Used to populate class, position, and player tables
##master_players = create_player_dataframe(2024)
##master_players = master_players.withColumn("Player", when(col("Player").contains('"'), ##remove_quotes_udf(col("Player"))).otherwise(col("Player")))
##master_players = master_players.withColumn("Pos", when(col("Pos").isNull(), "N/A").otherwise(col("Pos")))
##master_players = master_players.withColumn("Class", when(col("Class").isNull(), "N/A").otherwise(col("Class")))
##master_players = master_players.sort("Player")
##master_players = master_players.withColumnRenamed("Player", "player_name")
##master_players = master_players.withColumnRenamed("Class", "class_abbr")
##master_players = master_players.withColumnRenamed("Pos", "position_abbr")

# ETL Process for position table
##positions = master_players.select("position_abbr").distinct()
##positions = positions.sort("position_abbr")
##positions = positions.withColumn("position_name", map_position_name_udf(col("position_abbr")))
##positions.write.format("jdbc").options(**jdbc_options, dbtable="`position`").mode("append").save()

# ETL Process for class table
##classes = master_players.select("class_abbr").distinct()
##classes = classes.sort("class_abbr")
##classes = classes.withColumn("class_name", map_class_name_udf(col("class_abbr")))
##classes.write.format("jdbc").options(**jdbc_options, dbtable="class").mode("append").save()

# ETL Process for player table
##players = master_players.select("player_name", "position_abbr")
positions = spark.read.format("jdbc").options(**jdbc_options, dbtable="`position`").load()
##players = players.join(positions, "position_abbr", "inner")
##players = players.select("player_name", "position_id")
##players = players.sort("player_name")
##players.write.format("jdbc").options(**jdbc_options, dbtable="player").mode("append").save()

# ETL Process for player table
##rosters = master_players.select("player_name", "position_abbr", "college_name", "year", "class_abbr")
# Read players from SQL and format for join
players = spark.read.format("jdbc").options(**jdbc_options, dbtable="player").load()
players = players.join(positions, "position_id", "inner")
# Read teams from SQL and format for join
teams = spark.read.format("jdbc").options(**jdbc_options, dbtable="team").load()
teams = teams.join(colleges, "college_id", "inner")
teams = teams.join(conferences, "conference_id", "inner")
# Read classes from SQL
classes = spark.read.format("jdbc").options(**jdbc_options, dbtable="class").load()
# Join rosters with players, teams, and classes
##rosters = rosters.join(players, on=["player_name", "position_abbr"], how="inner")
##rosters = rosters.join(teams, on=["college_name", "year"], how="inner")
##rosters = rosters.join(classes, on=["class_abbr"], how="inner")
##rosters = rosters.select("player_id", "class_id", "team_id")
##rosters = rosters.sort("team_id", "player_id",)
##rosters.write.format("jdbc").options(**jdbc_options, dbtable="roster").mode("append").save()

# ETL Process for rushing table
rushing = master_college_conf.select(*master_college_conf.columns[1:10])
rushing = rushing.withColumnRenamed("Player", "player_name")
rushing = rushing.withColumnRenamed("Team", "college_name")
rushing = rushing.withColumnRenamed("Conf", "conference_shorthand")
rushing = rushing.withColumnRenamed("G", "games_played")
rushing = rushing.withColumnRenamed("Att", "rush_attempts")
rushing = rushing.withColumnRenamed("Yds6", "rush_yards")
rushing = rushing.withColumnRenamed("Y/A", "rush_yards_per_att")
rushing = rushing.withColumnRenamed("TD8", "rush_touchdowns")
rushing = rushing.withColumnRenamed("Y/G9", "rush_yards_per_game")
rushing = rushing.withColumn("player_name", when(col("player_name").contains('*'), remove_asterisk_udf(col("player_name"))).otherwise(col("player_name")))
rushing = rushing.withColumn("player_name", when(col("player_name").contains('"'), remove_quotes_udf(col("player_name"))).otherwise(col("player_name")))
rushing = rushing.withColumn("year", lit(2024))
# Read rosters from SQL and format for join
rosters = spark.read.format("jdbc").options(**jdbc_options, dbtable="roster").load()
rosters = rosters.join(players, "player_id", "inner")
rosters = rosters.join(teams, "team_id", "inner")
rosters = rosters.join(classes, "class_id", "inner")
# Join rushing with rosters
rushing = rushing.join(rosters, on=["player_name", "college_name", "year"], how="inner")
rushing = rushing.select("roster_id", "games_played", "rush_attempts", "rush_yards", "rush_yards_per_att", "rush_touchdowns", "rush_yards_per_game")
rushing = rushing.sort("rush_yards", ascending=False)
rushing.write.format("jdbc").options(**jdbc_options, dbtable="rushing_stat").mode("append").save()



rushing.show()

rosters.printSchema()

spark.stop()