import pandas as pd

class Processor:
    def __init__(self, cnx):
        self._cnx = cnx

    def extract_sql(self, db_table):
        return pd.read_sql(f"SELECT * FROM {db_table}", self._cnx)

    def extract_sql_query(self, query):
        return pd.read_sql(query, self._cnx)
    
    def extract_team_roster(self, college_name, year):
        query = f"""
                SELECT
                    p.player_name AS Player,
                    pos.position_abbr AS POS,
                    c.class_abbr, AS Class,
                FROM roster r
                INNER JOIN class c ON r.class_id = c.class_id
                INNER JOIN player p ON r.player_id = p.player_id
                INNER JOIN `position` pos ON p.position_id = pos.position_id
                INNER JOIN team t ON r.team_id = t.team_id
                INNER JOIN college col ON t.college_id = col.college_id
                WHERE col.college_name = "{college_name}" AND t.`year` = {year}
                ORDER BY pos.position_abbr, p.player_name asc;
                """
        return pd.read_sql(query, self._cnx)
        