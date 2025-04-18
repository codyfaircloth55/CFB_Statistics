from dash import Dash, dash_table, dcc, html, callback, Input, Output
from dash.exceptions import PreventUpdate
from dynaconf import Dynaconf
from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from processor import Processor

app = Dash()

# Helper Functions
def build_engine():
    settings = Dynaconf(envvar_prefix="DB", load_dotenv=True)
    return create_engine(settings.ENGINE_URL, echo=False)

def load_data(db_table):
    engine = build_engine()
    with engine.connect() as cnx:
        processor = Processor(cnx)
        return processor.extract_sql(db_table)

def load_data_query(query):
    engine = build_engine()
    with engine.connect() as cnx:
        processor = Processor(cnx)
        return processor.extract_sql_query(query)

def create_options(df, column_name):
    options = []
    options.extend(df[column_name].unique())
    return options

# Load Data
colleges = load_data('college')
teams = load_data('team')
positions = load_data('position')
classes = load_data('class')
conferences = load_data('conference')

# Maps for tooltips
position_map = positions.set_index("position_abbr")["position_name"].to_dict()
class_map = classes.set_index("class_abbr")["class_name"].to_dict()

# By team subtabs
def render_team_stats_tab():
    return [
        html.Div(
            [
                html.H3(id='team_offense_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_team_offense_table',
                    page_size=1,
                    sort_action='native',
                    tooltip_header={
                        "GP": "Games Played",
                        "Pts": "Points",
                        "Cmp": "Completions",
                        "PAtt": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "PYds": "Passing Yards",
                        "PTD": "Passing Touchdowns",
                        "RAtt": "Rushing Attempts",
                        "RYds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "RTD": "Rushing Touchdowns",
                        "Plays": "Total Plays",
                        "TYds": "Total Yards",
                        "Y/P": "Yards Per Play",
                        "PFD": "Passing First Downs",
                        "RFD": "Rushing First Downs",
                        "PenFD": "Penalty First Downs",
                        "TFD": "Total First Downs",
                        "Pen": "Penalties",
                        "PenYds": "Penalty Yards",
                        "Fum": "Fumbles Lost",
                        "Int": "Interceptions",
                        "TO": "Turnovers"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '4.347%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },

                )
            ]
        ),
        html.Div(
            [
                html.H3(id='team_defense_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_team_defense_table',
                    page_size=1,
                    sort_action='native',
                    tooltip_header={
                        "GP": "Games Played",
                        "Pts": "Points",
                        "Cmp": "Completions",
                        "PAtt": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "PYds": "Passing Yards",
                        "PTD": "Passing Touchdowns",
                        "RAtt": "Rushing Attempts",
                        "RYds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "RTD": "Rushing Touchdowns",
                        "Plays": "Total Plays",
                        "TYds": "Total Yards",
                        "Y/P": "Yards Per Play",
                        "PFD": "Passing First Downs",
                        "RFD": "Rushing First Downs",
                        "PenFD": "Penalty First Downs",
                        "TFD": "Total First Downs",
                        "Pen": "Penalties",
                        "PenYds": "Penalty Yards",
                        "Fum": "Fumbles Lost",
                        "Int": "Interceptions",
                        "TO": "Turnovers"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '4.347%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='team_special_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_team_special_table',
                    page_size=1,
                    sort_action='native',
                    tooltip_header={
                        "GP": "Games Played",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "XP%": "Extra Point Percentage",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "FG%": "Field Goal Percentage",
                        "KPts": "Kicking Points Scored",
                        "Punt": "Punts",
                        "PuntYds": "Punt Yards",
                        "Y/Punt": "Yards Per Punt",
                        "KR": "Kickoff Returns",
                        "KRYds": "Kickoff Return Yards",
                        "Y/KR": "Yards Per Kickoff Return",
                        "KRTD": "Kickoff Return Touchdowns",
                        "PR": "Punt Returns",
                        "PRYds": "Punt Return Yards",
                        "Y/PR": "Yards Per Punt Return",
                        "PRTD": "Punt Return Touchdowns"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '5.263%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },

                )
            ]
        )
    ]

def render_individual_stats_tab():
    return [
        html.Div(
            [
                html.H3(id='individual_rushing_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_individual_rushing_table',
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "GP": "Games Played",
                        "Att": "Rushing Attempts",
                        "Yds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "TD": "Rushing Touchdowns",
                        "Y/G": "Rushing Yards Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '14.285%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='individual_receiving_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_individual_receiving_table',
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "GP": "Games Played",
                        "Rec": "Receptions",
                        "Yds": "Receiving Yards",
                        "Y/R": "Receiving Yards Per Reception",
                        "TD": "Receiving Touchdowns",
                        "Y/G": "Receiving Yards Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '14.285%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='individual_passing_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_individual_passing_table',
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "GP": "Games Played",
                        "Cmp": "Completions",
                        "Att": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "Yds": "Passing Yards",
                        "TD": "Passing Touchdowns",
                        "TD%": "Touchdown Percentage",
                        "Int": "Interceptions",
                        "Int%": "Interception Percentage",
                        "Y/A": "Passing Yards Per Attempt",
                        "Y/C": "Passing Yards Per Completion",
                        "Y/G": "Passing Yards Per Game",
                        "Rating": "Passer Rating"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '7.142%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='individual_kicking_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_individual_kicking_table',
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "GP": "Games Played",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "XP%": "Extra Point Percentage",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "FG%": "Field Goal Percentage",
                        "Pts": "Points Scored"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '11.111%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='individual_punting_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_individual_punting_table',
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "GP": "Games Played",
                        "Punt": "Punts",
                        "Yds": "Punt Yards",
                        "Y/P": "Yards Per Punt"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '20%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='individual_scoring_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_college_individual_scoring_table',
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "GP": "Games Played",
                        "RTD": "Rushing Touchdowns",
                        "RecTD": "Receiving Touchdowns",
                        "PRTD": "Punt Return Touchdowns",
                        "KRTD": "Kickoff Return Touchdowns",
                        "FRTD": "Fumble Recovery Touchdowns",
                        "ITD": "Interception Return Touchdowns",
                        "OTD": "Other Touchdowns",
                        "TTD": "Total Touchdowns",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "2Pt": "Two Point Conversions Made",
                        "SFY": "Safeties",
                        "Pts": "Points Scored",
                        "P/G": "Points Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '5.555%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        )
    ]

# By conference subtabs
def render_conference_team_stats_tab():
    return [
        html.Div(
            [
                html.H3(id='conference_team_offense_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_team_offense_table',
                    sort_action='native',
                    tooltip_header={
                        "College": "College",
                        "GP": "Games Played",
                        "Pts": "Points",
                        "Cmp": "Completions",
                        "PAtt": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "PYds": "Passing Yards",
                        "PTD": "Passing Touchdowns",
                        "RAtt": "Rushing Attempts",
                        "RYds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "RTD": "Rushing Touchdowns",
                        "Plays": "Total Plays",
                        "TYds": "Total Yards",
                        "Y/P": "Yards Per Play",
                        "PFD": "Passing First Downs",
                        "RFD": "Rushing First Downs",
                        "PenFD": "Penalty First Downs",
                        "TFD": "Total First Downs",
                        "Pen": "Penalties",
                        "PenYds": "Penalty Yards",
                        "Fum": "Fumbles Lost",
                        "Int": "Interceptions",
                        "TO": "Turnovers"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '4.166%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='conference_team_defense_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_team_defense_table',
                    sort_action='native',
                    tooltip_header={
                        "College": "College",
                        "GP": "Games Played",
                        "Pts": "Points",
                        "Cmp": "Completions",
                        "PAtt": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "PYds": "Passing Yards",
                        "PTD": "Passing Touchdowns",
                        "RAtt": "Rushing Attempts",
                        "RYds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "RTD": "Rushing Touchdowns",
                        "Plays": "Total Plays",
                        "TYds": "Total Yards",
                        "Y/P": "Yards Per Play",
                        "PFD": "Passing First Downs",
                        "RFD": "Rushing First Downs",
                        "PenFD": "Penalty First Downs",
                        "TFD": "Total First Downs",
                        "Pen": "Penalties",
                        "PenYds": "Penalty Yards",
                        "Fum": "Fumbles Lost",
                        "Int": "Interceptions",
                        "TO": "Turnovers"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '4.166%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='conference_team_special_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_team_special_table',
                    sort_action='native',
                    tooltip_header={
                        "College": "College",
                        "GP": "Games Played",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "XP%": "Extra Point Percentage",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "FG%": "Field Goal Percentage",
                        "KPts": "Kicking Points Scored",
                        "Punt": "Punts",
                        "PuntYds": "Punt Yards",
                        "Y/Punt": "Yards Per Punt",
                        "KR": "Kickoff Returns",
                        "KRYds": "Kickoff Return Yards",
                        "Y/KR": "Yards Per Kickoff Return",
                        "KRTD": "Kickoff Return Touchdowns",
                        "PR": "Punt Returns",
                        "PRYds": "Punt Return Yards",
                        "Y/PR": "Yards Per Punt Return",
                        "PRTD": "Punt Return Touchdowns"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '5%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        )
    ]

def render_conference_individual_stats_tab():
    return [
        html.Div(
            [
                html.H3(id='conference_individual_rushing_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_individual_rushing_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "GP": "Games Played",
                        "Att": "Rushing Attempts",
                        "Yds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "TD": "Rushing Touchdowns",
                        "Y/G": "Rushing Yards Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '12.5%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='conference_individual_receiving_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_individual_receiving_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "GP": "Games Played",
                        "Rec": "Receptions",
                        "Yds": "Receiving Yards",
                        "Y/R": "Receiving Yards Per Reception",
                        "TD": "Receiving Touchdowns",
                        "Y/G": "Receiving Yards Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '12.5%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='conference_individual_passing_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_individual_passing_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "GP": "Games Played",
                        "Cmp": "Completions",
                        "Att": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "Yds": "Passing Yards",
                        "TD": "Passing Touchdowns",
                        "TD%": "Touchdown Percentage",
                        "Int": "Interceptions",
                        "Int%": "Interception Percentage",
                        "Y/A": "Passing Yards Per Attempt",
                        "Y/C": "Passing Yards Per Completion",
                        "Y/G": "Passing Yards Per Game",
                        "Rating": "Passer Rating"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '6.666%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='conference_individual_kicking_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_individual_kicking_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "GP": "Games Played",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "XP%": "Extra Point Percentage",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "FG%": "Field Goal Percentage",
                        "Pts": "Points Scored"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '10%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='conference_individual_punting_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_individual_punting_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "GP": "Games Played",
                        "Punt": "Punts",
                        "Yds": "Punt Yards",
                        "Y/P": "Yards Per Punt"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '16.666%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='conference_individual_scoring_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='by_conference_individual_scoring_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "GP": "Games Played",
                        "RTD": "Rushing Touchdowns",
                        "RecTD": "Receiving Touchdowns",
                        "PRTD": "Punt Return Touchdowns",
                        "KRTD": "Kickoff Return Touchdowns",
                        "FRTD": "Fumble Recovery Touchdowns",
                        "ITD": "Interception Return Touchdowns",
                        "OTD": "Other Touchdowns",
                        "TTD": "Total Touchdowns",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "2Pt": "Two Point Conversions Made",
                        "SFY": "Safeties",
                        "Pts": "Points Scored",
                        "P/G": "Points Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '5.263%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        )
    ]

# National subtabs
def render_national_team_stats_tab():
    return[
        html.Div(
            [
                html.H3(id='national_team_offense_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_team_offense_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "Pts": "Points",
                        "Cmp": "Completions",
                        "PAtt": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "PYds": "Passing Yards",
                        "PTD": "Passing Touchdowns",
                        "RAtt": "Rushing Attempts",
                        "RYds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "RTD": "Rushing Touchdowns",
                        "Plays": "Total Plays",
                        "TYds": "Total Yards",
                        "Y/P": "Yards Per Play",
                        "PFD": "Passing First Downs",
                        "RFD": "Rushing First Downs",
                        "PenFD": "Penalty First Downs",
                        "TFD": "Total First Downs",
                        "Pen": "Penalties",
                        "PenYds": "Penalty Yards",
                        "Fum": "Fumbles Lost",
                        "Int": "Interceptions",
                        "TO": "Turnovers"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '4%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='national_team_defense_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_team_defense_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "Pts": "Points",
                        "Cmp": "Completions",
                        "PAtt": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "PYds": "Passing Yards",
                        "PTD": "Passing Touchdowns",
                        "RAtt": "Rushing Attempts",
                        "RYds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "RTD": "Rushing Touchdowns",
                        "Plays": "Total Plays",
                        "TYds": "Total Yards",
                        "Y/P": "Yards Per Play",
                        "PFD": "Passing First Downs",
                        "RFD": "Rushing First Downs",
                        "PenFD": "Penalty First Downs",
                        "TFD": "Total First Downs",
                        "Pen": "Penalties",
                        "PenYds": "Penalty Yards",
                        "Fum": "Fumbles Lost",
                        "Int": "Interceptions",
                        "TO": "Turnovers"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '4%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='national_team_special_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_team_special_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "XP%": "Extra Point Percentage",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "FG%": "Field Goal Percentage",
                        "KPts": "Kicking Points Scored",
                        "Punt": "Punts",
                        "PuntYds": "Punt Yards",
                        "Y/Punt": "Yards Per Punt",
                        "KR": "Kickoff Returns",
                        "KRYds": "Kickoff Return Yards",
                        "Y/KR": "Yards Per Kickoff Return",
                        "KRTD": "Kickoff Return Touchdowns",
                        "PR": "Punt Returns",
                        "PRYds": "Punt Return Yards",
                        "Y/PR": "Yards Per Punt Return",
                        "PRTD": "Punt Return Touchdowns"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '4.761%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        )

    ]

def render_national_individual_stats_tab():
    return[
        html.Div(
            [
                html.H3(id='national_individual_rushing_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_individual_rushing_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "Att": "Rushing Attempts",
                        "Yds": "Rushing Yards",
                        "Y/A": "Rushing Yards Per Attempt",
                        "TD": "Rushing Touchdowns",
                        "Y/G": "Rushing Yards Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '11.111%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='national_individual_receiving_title')

            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_individual_receiving_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "Rec": "Receptions",
                        "Yds": "Receiving Yards",
                        "Y/R": "Receiving Yards Per Reception",
                        "TD": "Receiving Touchdowns",
                        "Y/G": "Receiving Yards Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '11.111%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='national_individual_passing_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_individual_passing_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "Cmp": "Completions",
                        "Att": "Passing Attempts",
                        "Cmp%": "Completion Percentage",
                        "Yds": "Passing Yards",
                        "TD": "Passing Touchdowns",
                        "TD%": "Touchdown Percentage",
                        "Int": "Interceptions",
                        "Int%": "Interception Percentage",
                        "Y/A": "Passing Yards Per Attempt",
                        "Y/C": "Passing Yards Per Completion",
                        "Y/G": "Passing Yards Per Game",
                        "Rating": "Passer Rating"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '6.25%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='national_individual_kicking_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_individual_kicking_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "XP%": "Extra Point Percentage",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "FG%": "Field Goal Percentage",
                        "Pts": "Points Scored"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '9.09%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='national_individual_punting_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_individual_punting_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "Punt": "Punts",
                        "Yds": "Punt Yards",
                        "Y/P": "Yards Per Punt"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '14.285%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        ),
        html.Div(
            [
                html.H3(id='national_individual_scoring_title')
            ]
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_individual_scoring_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "Player": "Player",
                        "College": "College",
                        "Conf": "Conference",
                        "GP": "Games Played",
                        "RTD": "Rushing Touchdowns",
                        "RecTD": "Receiving Touchdowns",
                        "PRTD": "Punt Return Touchdowns",
                        "KRTD": "Kickoff Return Touchdowns",
                        "FRTD": "Fumble Recovery Touchdowns",
                        "ITD": "Interception Return Touchdowns",
                        "OTD": "Other Touchdowns",
                        "TTD": "Total Touchdowns",
                        "XPM": "Extra Points Made",
                        "XPA": "Extra Points Attempted",
                        "FGM": "Field Goals Made",
                        "FGA": "Field Goals Attempted",
                        "2Pt": "Two Point Conversions Made",
                        "SFY": "Safeties",
                        "Pts": "Points Scored",
                        "P/G": "Points Per Game"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '5%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ]
        )
    ]

# Main Tabs
def render_by_team_tab():
    return [
        html.Div(
            [
                html.Label("Select a Year"),
                dcc.Dropdown(
                    id='year_dropdown',
                    options = create_options(teams, 'year'),
                    value = teams['year'].unique()[0],
                    multi = False,
                    clearable = False,
                )
            ],
            style={"width": "48%", "display": "inline-block", "padding": "1%"}
        ),
        html.Div(
            [
                html.Label("Select a College"),
                dcc.Dropdown(
                    id='college_dropdown',
                    multi = False,
                    clearable = False,


                )
            ],
            style={"width": "48%", "display": "inline-block", "padding": "1%"}
        ),
        html.Div(
            [
                html.Img(src='assets/air_force.png', style={'height': '10%', 'width': '10%', 'align': 'center'}),
            ]
        ),
        html.Div(
            [
                html.H2(id='college_page_title')
            ],
            style={'textAlign': 'center'}
        ),
        html.Div(
            [
                html.P(
                    id='college_page_subtitle')
            ],
            style={'textAlign': 'center'}
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='roster_table',
                    page_size=25,
                    sort_action='native',
                    style_cell={
                        'textAlign': 'center',
                        'width': '33.33%'
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ],
            style={'padding': '1%'}
        ),
        dcc.Tabs(
            [
                dcc.Tab(render_team_stats_tab(), label="Team Stats"),
                dcc.Tab(render_individual_stats_tab(), label="Individual Stats"),
            ]
        )
    ]

def render_by_conference_tab():
    return [
        html.Div(
            [
                html.Label("Select a Year"),
                dcc.Dropdown(
                    id='conference_year_dropdown',
                    options = create_options(teams, 'year'),
                    value = teams['year'].unique()[0],
                    multi = False,
                    clearable = False,
                )
            ],
            style={"width": "48%", "display": "inline-block", "padding": "1%"}
        ),
        html.Div(
            [
                html.Label("Select a Conference"),
                dcc.Dropdown(
                    id="conference_dropdown",
                    multi = False,
                    clearable = False,
                )
            ],
            style={"width": "48%", "display": "inline-block", "padding": "1%"}
        ),
        html.Div(
            [
                html.H2(id='conferences_page_title')
            ],
            style={'textAlign': 'center'}
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='conference_teams_table',
                    sort_action='native',
                    tooltip_header={
                        "College": "College",
                        "TW": "Total Wins",
                        "TL": "Total Losses",
                        "TW%": "Total Win Percentage",
                        "CW": "Conference Wins",
                        "CL": "Conference Losses",
                        "CW%": "Conference Win Percentage",
                        "PS/G": "Points Scored Per Game",
                        "PA/G": "Points Allowed Per Game",
                        "SRS": "Simple Rating System",
                        "SOS": "Strength of Schedule",
                        "APPre": "AP Preseason Rank",
                        "APHigh": "AP Highest Rank",
                        "APPost": "AP Final Rank"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '7.142%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    }
                )
            ],
            style={'padding': '1%'}
        ),
        dcc.Tabs(
            [
                dcc.Tab(render_conference_team_stats_tab(), label="Team Stats"),
                dcc.Tab(render_conference_individual_stats_tab(), label="Individual Stats"),
            ]
        )

    ]

def render_national_tab():
    return [
        html.Div(
            [
                html.Label("Select a Year"),
                dcc.Dropdown(
                    id='national_year_dropdown',
                    options = create_options(teams, 'year'),
                    value = teams['year'].unique()[0],
                    multi = False,
                    clearable = False,
                )
            ],
            style={"padding": "1%"}
        ),
        html.Div(
            [
                html.H2(id='national_page_title')
            ],
            style={'textAlign': 'center'}
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id='national_teams_table',
                    page_size=25,
                    sort_action='native',
                    tooltip_header={
                        "College": "College",
                        "Conf": "Conference",
                        "TW": "Total Wins",
                        "TL": "Total Losses",
                        "TW%": "Total Win Percentage",
                        "CW": "Conference Wins",
                        "CL": "Conference Losses",
                        "CW%": "Conference Win Percentage",
                        "PS/G": "Points Scored Per Game",
                        "PA/G": "Points Allowed Per Game",
                        "SRS": "Simple Rating System",
                        "SOS": "Strength of Schedule",
                        "APPre": "AP Preseason Rank",
                        "APHigh": "AP Highest Rank",
                        "APPost": "AP Final Rank"
                    },
                    tooltip_delay=0,
                    tooltip_duration=None,
                    style_cell={
                        'textAlign': 'center',
                        'width': '6.666%'
                    },
                    style_header={
                        'backgroundColor': 'lightgrey',
                        'fontWeight': 'bold',
                        'textAlign': 'center'
                    },
                )
            ],
            style={'padding': '1%'}
        ),
        dcc.Tabs(
            [
                dcc.Tab(render_national_team_stats_tab(), label="Team Stats"),
                dcc.Tab(render_national_individual_stats_tab(), label="Individual Stats"),
            ]
        ),
    ]

app.layout =[
    html.Div(
        [
            html.H1("College Football Dashboard"),
        ],
        style={'textAlign': 'center'}
    ),
    dcc.Tabs(
        [
            dcc.Tab(render_by_team_tab(), label="Stats by Team"),
            dcc.Tab(render_by_conference_tab(), label="Stats by Conference"),
            dcc.Tab(render_national_tab(), label="National Stats"),
        ]
    ),
]

# Callbacks for by team tab
@callback(
    Output(component_id='college_dropdown', component_property='options'),
    Output(component_id='college_dropdown', component_property='value'),
    Input(component_id='year_dropdown', component_property='value')
)
def update_college_dropdown(selected_year):
    if not selected_year:
        raise PreventUpdate
    colleges_df = teams.merge(colleges, on='college_id', how='inner')
    colleges_df = colleges_df[colleges_df['year'] == selected_year]
    colleges_df = colleges_df.drop_duplicates(subset=['college_name'])
    colleges_df = colleges_df.sort_values(by='college_name')
    options = create_options(colleges_df, 'college_name')
    value = options[0]
    return options, value

@callback(
    Output(component_id='college_page_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_college_page_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    selected_college_df = colleges[colleges['college_name'] == selected_college]
    college_mascot = selected_college_df["college_mascot"].values[0]
    title = f"{selected_year} {selected_college} {college_mascot}"
    return title

@callback(
    Output(component_id='college_page_subtitle', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_college_page_subtitle(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    selected_college_df = teams.merge(colleges, on='college_id', how='inner')
    selected_college_df = selected_college_df.merge(conferences, on='conference_id', how='inner')
    selected_college_df = selected_college_df[selected_college_df['college_name'] == selected_college]
    selected_college_df = selected_college_df[selected_college_df['year'] == selected_year]
    conference_name = selected_college_df['conference_name'].values[0]
    conference_abbr = selected_college_df['conference_shorthand'].values[0]
    subtitle = f"Conference: {conference_name} ({conference_abbr})"
    return subtitle

@callback(
    Output(component_id='roster_table', component_property='data'),
    Output(component_id='roster_table', component_property='columns'),
    Output(component_id='roster_table', component_property='tooltip_data'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_roster_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select 
                p.player_name as Player,
                pos.position_abbr as POS,
                c.class_abbr as Class
            from roster r
            inner join class c on r.class_id = c.class_id
            inner join player p on r.player_id = p.player_id
            inner join `position` pos on p.position_id = pos.position_id
            inner join team t on r.team_id = t.team_id
            inner join college col on t.college_id = col.college_id
            where col.college_name = "{selected_college}" and t.`year` = {selected_year}
            order by position_abbr, player_name asc;
            """
    df = load_data_query(query)

    columns = [
        {"name": "Player", "id": "Player"},
        {"name": "POS", "id": "POS"},
        {"name": "Class", "id": "Class"}
    ]

    tooltip_data = [
        {
            'Class': {'value': class_map.get(row['Class'], row['Class']), 'type': 'text'},
            'POS': {'value': position_map.get(row['POS'], row['POS']), 'type': 'text'}
        }
        for _, row in df.iterrows()
    ]

    return df.to_dict('records'), columns, tooltip_data

# - Callbacks for by team/team stats tab
@callback(
        Output(component_id='team_offense_title', component_property='children'),
        Input(component_id='year_dropdown', component_property='value'),
        Input(component_id='college_dropdown', component_property='value')
)
def update_team_offense_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Offense (Per Game Stats)"

@callback(
    Output(component_id='by_college_team_offense_table', component_property='data'),
    Output(component_id='by_college_team_offense_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_team_offense_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                toff.games_played as GP,
                toff.points_per_game as Pts,
                toff.completions_per_game as Cmp,
                toff.passing_attempts_per_game as PAtt,
                toff.completion_percentage_per_game as "Cmp%",
                toff.passing_yards_per_game as PYds,
                toff.passing_touchdowns_per_game as PTD,
                toff.rushing_attempts_per_game as RAtt,
                toff.rushing_yards_per_game as RYds,
                toff.rushing_yards_per_attempt_per_game as "Y/A",
                toff.rushing_touchdowns_per_game as RTD,
                toff.total_plays_per_game as Plays,
                toff.total_yards_per_game as TYds,
                toff.yards_per_play_per_game as "Y/P",
                toff.passing_first_downs_per_game as PFD,
                toff.rushing_first_downs_per_game as RFD,
                toff.penalty_first_downs_per_game as PenFD,
                toff.total_first_downs_per_game as TFD,
                toff.penalties_per_game as Pen,
                toff.penalty_yards_per_game as PenYds,
                toff.fumbles_lost_per_game as Fum,
                toff.interceptions_per_game as `Int`,
                toff. turnovers_per_game as `TO`
            from team_offense toff
            inner join team t on toff.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where college_name = "{selected_college}" and `year` = {selected_year};
            """
    df = load_data_query(query)

    columns = [{'name': col, 'id': col} for col in df.columns]
    
    return df.to_dict('records'), columns

@callback(
    Output(component_id='team_defense_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_team_defense_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Defense (Opponent Per Game Stats)"

@callback(
    Output(component_id='by_college_team_defense_table', component_property='data'),
    Output(component_id='by_college_team_defense_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_team_defense_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                td.games_played as GP,
                td.opponent_points_per_game as Pts,
                td.opponent_completions_per_game as Cmp,
                td.opponent_passing_attempts_per_game as PAtt,
                td.opponent_completion_percentage_per_game as "Cmp%",
                td.opponent_passing_yards_per_game as PYds,
                td.opponent_passing_touchdowns_per_game as PTD,
                td.opponent_rushing_attempts_per_game as RAtt,
                td.opponent_rushing_yards_per_game as RYds,
                td.opponent_rushing_yards_per_attempt_per_game as "Y/A",
                td.opponent_rushing_touchdowns_per_game as RTD,
                td.opponent_total_plays_per_game as Plays,
                td.opponent_total_yards_per_game as TYds,
                td.opponent_yards_per_play_per_game as "Y/P",
                td.opponent_passing_first_downs_per_game as PFD,
                td.opponent_rushing_first_downs_per_game as RFD,
                td.opponent_penalty_first_downs_per_game as PenFD,
                td.opponent_total_first_downs_per_game as TFD,
                td.opponent_penalties_per_game as Pen,
                td.opponent_penalty_yards_per_game as PenYds,
                td.opponent_fumbles_lost_per_game as Fum,
                td.opponent_interceptions_per_game as `Int`,
                td.opponent_turnovers_per_game as `TO`
            from team_defense td
            inner join team t on td.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where c.college_name = "{selected_college}" and t.year = {selected_year};
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='team_special_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_team_special_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Special Teams (Per Game Stats)"

@callback(
    Output(component_id='by_college_team_special_table', component_property='data'),
    Output(component_id='by_college_team_special_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_team_special_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                ts.games_played as GP,
                ts.extra_points_made_per_game as XPM,
                ts.extra_points_attempted_per_game as XPA,
                ts.extra_point_percentage as "XP%",
                ts.field_goals_made_per_game as FGM,
                ts.field_goals_attempted_per_game as FGA,
                ts.field_goal_percentage as "FG%",
                ts.kicking_points_scored_per_game as KPts,
                ts.punts_per_game as Punt,
                ts.punt_yards_per_game as PuntYds,
                ts.yards_per_punt_per_game as "Y/Punt",
                ts.kickoff_returns_per_game as KR,
                ts.kickoff_return_yards_per_game as KRYds,
                ts.return_yards_per_kickoff_per_game as "Y/KR",
                ts.kickoff_return_touchdowns_per_game as KRTD,
                ts.punt_returns_per_game as PR,
                ts.punt_return_yards_per_game as PRYds,
                ts.return_yards_per_punt_per_game as "Y/PR",
                ts.punt_return_touchdowns_per_game as PRTD
            from team_special ts
            inner join team t on ts.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where c.college_name = "{selected_college}" and t.`year` = {selected_year};
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

# - Callbacks for by team/individual stats tab
@callback(
    Output(component_id='individual_rushing_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_rushing_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Rushing"

@callback(
    Output(component_id='by_college_individual_rushing_table', component_property='data'),
    Output(component_id='by_college_individual_rushing_table', component_property='page_size'),
    Output(component_id='by_college_individual_rushing_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_rushing_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                rs.games_played as GP,
                rs.rushing_attempts as Att,
                rs.rushing_yards as Yds,
                rs.rushing_yards_per_attempt as "Y/A",
                rs.rushing_touchdowns as TD,
                rs.rushing_yards_per_game as "Y/G"
            from rushing_stat rs
            inner join roster r on rs.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where c.college_name = "{selected_college}" and t.`year` = {selected_year}
            order by rs.rushing_yards desc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

@callback(
    Output(component_id='individual_receiving_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_receiving_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Receiving"

@callback(
    Output(component_id='by_college_individual_receiving_table', component_property='data'),
    Output(component_id='by_college_individual_receiving_table', component_property='page_size'),
    Output(component_id='by_college_individual_receiving_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_receiving_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                rs.games_played as GP,
                rs.receptions as Rec,
                rs.receiving_yards as Yds,
                rs.receiving_yards_per_reception as "Y/R",
                rs.receiving_touchdowns as TD,
                rs.receiving_yards_per_game as "Y/G"
            from receiving_stat rs
            inner join roster r on rs.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where c.college_name = "{selected_college}" and t.`year` = {selected_year}
            order by rs.receiving_yards desc; 
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

@callback(
    Output(component_id='individual_passing_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_passing_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Passing"

@callback(
    Output(component_id='by_college_individual_passing_table', component_property='data'),
    Output(component_id='by_college_individual_passing_table', component_property='page_size'),
    Output(component_id='by_college_individual_passing_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_passing_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                ps.games_played as GP,
                ps.completions as Cmp,
                ps.passing_attempts as Att,
                ps.completion_percentage as "Cmp%",
                ps.passing_yards as Yds,
                ps.passing_touchdowns as TD,
                ps.touchdown_percentage as "TD%",
                ps.interceptions as `Int`,
                ps.interception_percentage as "Int%",
                ps.passing_yards_per_attempt as "Y/A",
                ps.passing_yards_per_completion as "Y/C",
                ps.passing_yards_per_game as "Y/G",
                ps.passer_rating as Rating
            from passing_stat ps
            inner join roster r on ps.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where c.college_name = "{selected_college}" and t.`year` = {selected_year}
            order by ps.passing_yards desc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

@callback(
    Output(component_id='individual_kicking_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_kicking_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Kicking"

@callback(
    Output(component_id='by_college_individual_kicking_table', component_property='data'),
    Output(component_id='by_college_individual_kicking_table', component_property='page_size'),
    Output(component_id='by_college_individual_kicking_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_kicking_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                ks.games_played as GP,
                ks.extra_points_made as XPM,
                ks.extra_points_attempted as XPA,
                ks.extra_point_percentage as "XP%",
                ks.field_goals_made as FGM,
                ks.field_goals_attempted as FGA,
                ks.Field_goal_percentage as "FG%",
                ks.points_scored as Pts
            from kicking_stat ks
            inner join roster r on ks.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where c.college_name = "{selected_college}" and t.`year` = {selected_year}
            order by ks.points_scored desc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

@callback(
    Output(component_id='individual_punting_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_punting_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Punting"

@callback(
    Output(component_id='by_college_individual_punting_table', component_property='data'),
    Output(component_id='by_college_individual_punting_table', component_property='page_size'),
    Output(component_id='by_college_individual_punting_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_punting_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                ps.games_played as GP,
                ps.punts as Punt,
                ps.punt_yards as Yds,
                ps.yards_per_punt as "Y/P"
            from punting_stat ps
            inner join roster r on ps.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where c.college_name = "{selected_college}" and t.`year` = {selected_year}
            order by ps.punt_yards desc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

@callback(
    Output(component_id='individual_scoring_title', component_property='children'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_scoring_title(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    return f"{selected_year} {selected_college} Scoring"

@callback(
    Output(component_id='by_college_individual_scoring_table', component_property='data'),
    Output(component_id='by_college_individual_scoring_table', component_property='page_size'),
    Output(component_id='by_college_individual_scoring_table', component_property='columns'),
    Input(component_id='year_dropdown', component_property='value'),
    Input(component_id='college_dropdown', component_property='value')
)
def update_individual_scoring_table(selected_year, selected_college):
    if not selected_year or not selected_college:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                ss.games_played as GP,
                ss.rushing_touchdowns as RTD,
                ss.receiving_touchdowns as RecTD,
                ss.punt_return_touchdowns as PRTD,
                ss.kickoff_return_touchdowns as KRTD,
                ss.fumble_recovery_touchdowns as FRTD,
                ss.interception_return_touchdowns as ITD,
                ss.other_touchdowns as OTD,
                ss.total_touchdowns as TTD,
                ss.extra_points_made as XPM,
                ss.extra_points_attempted as XPA,
                ss.field_goals_made as FGM,
                ss.field_goals_attempted as FGA,
                ss.two_point_conversions_made as "2Pt",
                ss.safeties as SFY,
                ss.points_scored as Pts,
                ss.points_per_game as "P/G"
            from scoring_stat as ss
            inner join roster r on ss.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            where c.college_name = "{selected_college}" and t.`year` = {selected_year}
            order by ss.points_scored desc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

# Callbacks for by conference tab
@callback(
    Output(component_id='conference_dropdown', component_property='options'),
    Output(component_id='conference_dropdown', component_property='value'),
    Input(component_id='conference_year_dropdown', component_property='value')
)
def update_conference_dropdown(selected_year):
    if not selected_year:
        raise PreventUpdate
    conferences_df = teams.merge(conferences, on='conference_id', how='inner')
    conferences_df = conferences_df[conferences_df['year'] == selected_year]
    conferences_df = conferences_df.drop_duplicates(subset=['conference_name'])
    conferences_df = conferences_df.sort_values(by='conference_shorthand')
    options = create_options(conferences_df, 'conference_shorthand')
    value = options[0]
    return options, value

@callback(
    Output(component_id='conferences_page_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conferences_page_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    selected_conference_df = conferences[conferences['conference_shorthand'] == selected_conference]
    conference_name = selected_conference_df["conference_name"].values[0]
    title = f"{selected_year} {conference_name} ({selected_conference})"
    return title

@callback(
    Output(component_id='conference_teams_table', component_property='data'),
    Output(component_id='conference_teams_table', component_property='page_size'),
    Output(component_id='conference_teams_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_teams_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select 
                c.college_name as College,
                ts.total_wins as TW,
                ts.total_losses as TL,
                ts.total_win_percentage as "TW%",
                ts.conference_wins as CW,
                ts.conference_losses as CL,
                ts.conference_win_percentage as "CW%",
                ts.points_scored_per_game as "PS/G",
                ts.points_allowed_per_game as "PA/G",
                ts.simple_rating_system as SRS,
                ts.strength_of_schedule as SOS,
                ts.ap_preseason_rank as APPre,
                ts.ap_highest_rank as APHigh,
                ts.ap_final_rank as APPost
            from team_standing ts
            inner join team t on ts.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by ts.total_wins desc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

# - Callbacks for by conference/team stats tab
@callback(
    Output(component_id='conference_team_offense_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_team_offense_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Offense (Per Game Stats)"

@callback(
    Output(component_id='by_conference_team_offense_table', component_property='data'),
    Output(component_id='by_conference_team_offense_table', component_property='page_size'),
    Output(component_id='by_conference_team_offense_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_team_offense_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                c.college_name as College,
                toff.games_played as GP,
                toff.points_per_game as Pts,
                toff.completions_per_game as Cmp,
                toff.passing_attempts_per_game as PAtt,
                toff.completion_percentage_per_game as "Cmp%",
                toff.passing_yards_per_game as PYds,
                toff.passing_touchdowns_per_game as PTD,
                toff.rushing_attempts_per_game as RAtt,
                toff.rushing_yards_per_game as RYds,
                toff.rushing_yards_per_attempt_per_game as "Y/A",
                toff.rushing_touchdowns_per_game as RTD,
                toff.total_plays_per_game as Plays,
                toff.total_yards_per_game as TYds,
                toff.yards_per_play_per_game as "Y/P",
                toff.passing_first_downs_per_game as PFD,
                toff.rushing_first_downs_per_game as RFD,
                toff.penalty_first_downs_per_game as PenFD,
                toff.total_first_downs_per_game as TFD,
                toff.penalties_per_game as Pen,
                toff.penalty_yards_per_game as PenYds,
                toff.fumbles_lost_per_game as Fum,
                toff.interceptions_per_game as `Int`,
                toff. turnovers_per_game as `TO`
            from team_offense toff
            inner join team t on toff.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by toff.points_per_game desc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

@callback(
    Output(component_id='conference_team_defense_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_team_defense_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Defense (Opponent Per Game Stats)"

@callback(
    Output(component_id='by_conference_team_defense_table', component_property='data'),
    Output(component_id='by_conference_team_defense_table', component_property='page_size'),
    Output(component_id='by_conference_team_defense_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_team_defense_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                c.college_name as College,
                td.games_played as GP,
                td.opponent_points_per_game as Pts,
                td.opponent_completions_per_game as Cmp,
                td.opponent_passing_attempts_per_game as PAtt,
                td.opponent_completion_percentage_per_game as "Cmp%",
                td.opponent_passing_yards_per_game as PYds,
                td.opponent_passing_touchdowns_per_game as PTD,
                td.opponent_rushing_attempts_per_game as RAtt,
                td.opponent_rushing_yards_per_game as RYds,
                td.opponent_rushing_yards_per_attempt_per_game as "Y/A",
                td.opponent_rushing_touchdowns_per_game as RTD,
                td.opponent_total_plays_per_game as Plays,
                td.opponent_total_yards_per_game as TYds,
                td.opponent_yards_per_play_per_game as "Y/P",
                td.opponent_passing_first_downs_per_game as PFD,
                td.opponent_rushing_first_downs_per_game as RFD,
                td.opponent_penalty_first_downs_per_game as PenFD,
                td.opponent_total_first_downs_per_game as TFD,
                td.opponent_penalties_per_game as Pen,
                td.opponent_penalty_yards_per_game as PenYds,
                td.opponent_fumbles_lost_per_game as Fum,
                td.opponent_interceptions_per_game as `Int`,
                td.opponent_turnovers_per_game as `TO`
            from team_defense td
            inner join team t on td.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.year = {selected_year}
            order by td.opponent_points_per_game asc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

@callback(
    Output(component_id='conference_team_special_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_team_special_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Special Teams (Per Game Stats)"

@callback(
    Output(component_id='by_conference_team_special_table', component_property='data'),
    Output(component_id='by_conference_team_special_table', component_property='page_size'),
    Output(component_id='by_conference_team_special_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_team_special_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                c.college_name as College,
                ts.games_played as GP,
                ts.extra_points_made_per_game as XPM,
                ts.extra_points_attempted_per_game as XPA,
                ts.extra_point_percentage as "XP%",
                ts.field_goals_made_per_game as FGM,
                ts.field_goals_attempted_per_game as FGA,
                ts.field_goal_percentage as "FG%",
                ts.kicking_points_scored_per_game as KPts,
                ts.punts_per_game as Punt,
                ts.punt_yards_per_game as PuntYds,
                ts.yards_per_punt_per_game as "Y/Punt",
                ts.kickoff_returns_per_game as KR,
                ts.kickoff_return_yards_per_game as KRYds,
                ts.return_yards_per_kickoff_per_game as "Y/KR",
                ts.kickoff_return_touchdowns_per_game as KRTD,
                ts.punt_returns_per_game as PR,
                ts.punt_return_yards_per_game as PRYds,
                ts.return_yards_per_punt_per_game as "Y/PR",
                ts.punt_return_touchdowns_per_game as PRTD
            from team_special ts
            inner join team t on ts.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by c.college_name asc;
            """
    df = load_data_query(query)
    page_size = len(df)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), page_size, columns

# - Callbacks for by conference/individual stats tab
@callback(
    Output(component_id='conference_individual_rushing_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_rushing_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Rushing"

@callback(
    Output(component_id='by_conference_individual_rushing_table', component_property='data'),
    Output(component_id='by_conference_individual_rushing_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_rushing_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                rs.games_played as GP,
                rs.rushing_attempts as Att,
                rs.rushing_yards as Yds,
                rs.rushing_yards_per_attempt as "Y/A",
                rs.rushing_touchdowns as TD,
                rs.rushing_yards_per_game as "Y/G"
            from rushing_stat rs
            inner join roster r on rs.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by rs.rushing_yards desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='conference_individual_receiving_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_receiving_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Receiving"

@callback(
    Output(component_id='by_conference_individual_receiving_table', component_property='data'),
    Output(component_id='by_conference_individual_receiving_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_receiving_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                rs.games_played as GP,
                rs.receptions as Rec,
                rs.receiving_yards as Yds,
                rs.receiving_yards_per_reception as "Y/R",
                rs.receiving_touchdowns as TD,
                rs.receiving_yards_per_game as "Y/G"
            from receiving_stat rs
            inner join roster r on rs.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by rs.receiving_yards desc; 
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='conference_individual_passing_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_passing_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Passing"

@callback(
    Output(component_id='by_conference_individual_passing_table', component_property='data'),
    Output(component_id='by_conference_individual_passing_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_passing_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                ps.games_played as GP,
                ps.completions as Cmp,
                ps.passing_attempts as Att,
                ps.completion_percentage as "Cmp%",
                ps.passing_yards as Yds,
                ps.passing_touchdowns as TD,
                ps.touchdown_percentage as "TD%",
                ps.interceptions as `Int`,
                ps.interception_percentage as "Int%",
                ps.passing_yards_per_attempt as "Y/A",
                ps.passing_yards_per_completion as "Y/C",
                ps.passing_yards_per_game as "Y/G",
                ps.passer_rating as Rating
            from passing_stat ps
            inner join roster r on ps.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by ps.passing_yards desc; 
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='conference_individual_kicking_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_kicking_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Kicking"

@callback(
    Output(component_id='by_conference_individual_kicking_table', component_property='data'),
    Output(component_id='by_conference_individual_kicking_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_kicking_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                ks.games_played as GP,
                ks.extra_points_made as XPM,
                ks.extra_points_attempted as XPA,
                ks.extra_point_percentage as "XP%",
                ks.field_goals_made as FGM,
                ks.field_goals_attempted as FGA,
                ks.Field_goal_percentage as "FG%",
                ks.points_scored as Pts
            from kicking_stat ks
            inner join roster r on ks.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by ks.points_scored desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='conference_individual_punting_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_punting_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Punting"

@callback(
    Output(component_id='by_conference_individual_punting_table', component_property='data'),
    Output(component_id='by_conference_individual_punting_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_indidual_punting_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                ps.games_played as GP,
                ps.punts as Punt,
                ps.punt_yards as Yds,
                ps.yards_per_punt as "Y/P"
            from punting_stat ps
            inner join roster r on ps.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by ps.punt_yards desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='conference_individual_scoring_title', component_property='children'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_scoring_title(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    return f"{selected_year} {selected_conference} Scoring"

@callback(
    Output(component_id='by_conference_individual_scoring_table', component_property='data'),
    Output(component_id='by_conference_individual_scoring_table', component_property='columns'),
    Input(component_id='conference_year_dropdown', component_property='value'),
    Input(component_id='conference_dropdown', component_property='value')
)
def update_conference_individual_scoring_table(selected_year, selected_conference):
    if not selected_year or not selected_conference:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                ss.games_played as GP,
                ss.rushing_touchdowns as RTD,
                ss.receiving_touchdowns as RecTD,
                ss.punt_return_touchdowns as PRTD,
                ss.kickoff_return_touchdowns as KRTD,
                ss.fumble_recovery_touchdowns as FRTD,
                ss.interception_return_touchdowns as ITD,
                ss.other_touchdowns as OTD,
                ss.total_touchdowns as TTD,
                ss.extra_points_made as XPM,
                ss.extra_points_attempted as XPA,
                ss.field_goals_made as FGM,
                ss.field_goals_attempted as FGA,
                ss.two_point_conversions_made as "2Pt",
                ss.safeties as SFY,
                ss.points_scored as Pts,
                ss.points_per_game as "P/G"
            from scoring_stat as ss
            inner join roster r on ss.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where con.conference_shorthand = "{selected_conference}" and t.`year` = {selected_year}
            order by ss.points_scored desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

# Callbacks for national tab
@callback(
    Output(component_id='national_page_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_page_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Stats"

@callback(
    Output(component_id='national_teams_table', component_property='data'),
    Output(component_id='national_teams_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_teams_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select 
                c.college_name as College,
                con.conference_shorthand as Conf,
                ts.total_wins as TW,
                ts.total_losses as TL,
                ts.total_win_percentage as "TW%",
                ts.conference_wins as CW,
                ts.conference_losses as CL,
                ts.conference_win_percentage as "CW%",
                ts.points_scored_per_game as "PS/G",
                ts.points_allowed_per_game as "PA/G",
                ts.simple_rating_system as SRS,
                ts.strength_of_schedule as SOS,
                ts.ap_preseason_rank as APPre,
                ts.ap_highest_rank as APHigh,
                ts.ap_final_rank as APPost
            from team_standing ts
            inner join team t on ts.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by ts.total_wins desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

# - Callbacks for national/team stats tab
@callback(
    Output(component_id='national_team_offense_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_team_offense_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Offense (Per Game Stats)"

@callback(
    Output(component_id='national_team_offense_table', component_property='data'),
    Output(component_id='national_team_offense_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_team_offense_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                c.college_name as College,
                con.conference_shorthand as Conf,
                toff.games_played as GP,
                toff.points_per_game as Pts,
                toff.completions_per_game as Cmp,
                toff.passing_attempts_per_game as PAtt,
                toff.completion_percentage_per_game as "Cmp%",
                toff.passing_yards_per_game as PYds,
                toff.passing_touchdowns_per_game as PTD,
                toff.rushing_attempts_per_game as RAtt,
                toff.rushing_yards_per_game as RYds,
                toff.rushing_yards_per_attempt_per_game as "Y/A",
                toff.rushing_touchdowns_per_game as RTD,
                toff.total_plays_per_game as Plays,
                toff.total_yards_per_game as TYds,
                toff.yards_per_play_per_game as "Y/P",
                toff.passing_first_downs_per_game as PFD,
                toff.rushing_first_downs_per_game as RFD,
                toff.penalty_first_downs_per_game as PenFD,
                toff.total_first_downs_per_game as TFD,
                toff.penalties_per_game as Pen,
                toff.penalty_yards_per_game as PenYds,
                toff.fumbles_lost_per_game as Fum,
                toff.interceptions_per_game as `Int`,
                toff. turnovers_per_game as `TO`
            from team_offense toff
            inner join team t on toff.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by toff.points_per_game desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns


@callback(
    Output(component_id='national_team_defense_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_team_defense_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Defense (Opponent Per Game Stats)"

@callback(
    Output(component_id='national_team_defense_table', component_property='data'),
    Output(component_id='national_team_defense_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_team_defense_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                c.college_name as College,
                con.conference_shorthand as Conf,
                td.games_played as GP,
                td.opponent_points_per_game as Pts,
                td.opponent_completions_per_game as Cmp,
                td.opponent_passing_attempts_per_game as PAtt,
                td.opponent_completion_percentage_per_game as "Cmp%",
                td.opponent_passing_yards_per_game as PYds,
                td.opponent_passing_touchdowns_per_game as PTD,
                td.opponent_rushing_attempts_per_game as RAtt,
                td.opponent_rushing_yards_per_game as RYds,
                td.opponent_rushing_yards_per_attempt_per_game as "Y/A",
                td.opponent_rushing_touchdowns_per_game as RTD,
                td.opponent_total_plays_per_game as Plays,
                td.opponent_total_yards_per_game as TYds,
                td.opponent_yards_per_play_per_game as "Y/P",
                td.opponent_passing_first_downs_per_game as PFD,
                td.opponent_rushing_first_downs_per_game as RFD,
                td.opponent_penalty_first_downs_per_game as PenFD,
                td.opponent_total_first_downs_per_game as TFD,
                td.opponent_penalties_per_game as Pen,
                td.opponent_penalty_yards_per_game as PenYds,
                td.opponent_fumbles_lost_per_game as Fum,
                td.opponent_interceptions_per_game as `Int`,
                td.opponent_turnovers_per_game as `TO`
            from team_defense td
            inner join team t on td.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.year = {selected_year}
            order by td.opponent_points_per_game asc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='national_team_special_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_team_special_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Special Teams (Per Game Stats)"

@callback(
    Output(component_id='national_team_special_table', component_property='data'),
    Output(component_id='national_team_special_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_team_special_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                c.college_name as College,
                con.conference_shorthand as Conf,
                ts.games_played as GP,
                ts.extra_points_made_per_game as XPM,
                ts.extra_points_attempted_per_game as XPA,
                ts.extra_point_percentage as "XP%",
                ts.field_goals_made_per_game as FGM,
                ts.field_goals_attempted_per_game as FGA,
                ts.field_goal_percentage as "FG%",
                ts.kicking_points_scored_per_game as KPts,
                ts.punts_per_game as Punt,
                ts.punt_yards_per_game as PuntYds,
                ts.yards_per_punt_per_game as "Y/Punt",
                ts.kickoff_returns_per_game as KR,
                ts.kickoff_return_yards_per_game as KRYds,
                ts.return_yards_per_kickoff_per_game as "Y/KR",
                ts.kickoff_return_touchdowns_per_game as KRTD,
                ts.punt_returns_per_game as PR,
                ts.punt_return_yards_per_game as PRYds,
                ts.return_yards_per_punt_per_game as "Y/PR",
                ts.punt_return_touchdowns_per_game as PRTD
            from team_special ts
            inner join team t on ts.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by c.college_name asc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

# - Callbacks for national/individual stats tab
@callback(
    Output(component_id='national_individual_rushing_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_rushing_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Rushing"

@callback(
    Output(component_id='national_individual_rushing_table', component_property='data'),
    Output(component_id='national_individual_rushing_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_rushing_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                con.conference_shorthand as Conf,
                rs.games_played as GP,
                rs.rushing_attempts as Att,
                rs.rushing_yards as Yds,
                rs.rushing_yards_per_attempt as "Y/A",
                rs.rushing_touchdowns as TD,
                rs.rushing_yards_per_game as "Y/G"
            from rushing_stat rs
            inner join roster r on rs.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by rs.rushing_yards desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='national_individual_receiving_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_receiving_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Receiving"

@callback(
    Output(component_id='national_individual_receiving_table', component_property='data'),
    Output(component_id='national_individual_receiving_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_receiving_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                con.conference_shorthand as Conf,
                rs.games_played as GP,
                rs.receptions as Rec,
                rs.receiving_yards as Yds,
                rs.receiving_yards_per_reception as "Y/R",
                rs.receiving_touchdowns as TD,
                rs.receiving_yards_per_game as "Y/G"
            from receiving_stat rs
            inner join roster r on rs.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by rs.receiving_yards desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='national_individual_passing_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_passing_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Passing"

@callback(
    Output(component_id='national_individual_passing_table', component_property='data'),
    Output(component_id='national_individual_passing_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_passing_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                con.conference_shorthand as Conf,
                ps.games_played as GP,
                ps.completions as Cmp,
                ps.passing_attempts as Att,
                ps.completion_percentage as "Cmp%",
                ps.passing_yards as Yds,
                ps.passing_touchdowns as TD,
                ps.touchdown_percentage as "TD%",
                ps.interceptions as `Int`,
                ps.interception_percentage as "Int%",
                ps.passing_yards_per_attempt as "Y/A",
                ps.passing_yards_per_completion as "Y/C",
                ps.passing_yards_per_game as "Y/G",
                ps.passer_rating as Rating
            from passing_stat ps
            inner join roster r on ps.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by ps.passing_yards desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='national_individual_kicking_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_kicking_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Kicking"

@callback(
    Output(component_id='national_individual_kicking_table', component_property='data'),
    Output(component_id='national_individual_kicking_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_kicking_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                con.conference_shorthand as Conf,
                ks.games_played as GP,
                ks.extra_points_made as XPM,
                ks.extra_points_attempted as XPA,
                ks.extra_point_percentage as "XP%",
                ks.field_goals_made as FGM,
                ks.field_goals_attempted as FGA,
                ks.Field_goal_percentage as "FG%",
                ks.points_scored as Pts
            from kicking_stat ks
            inner join roster r on ks.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by ks.points_scored desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='national_individual_punting_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_punting_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Punting"

@callback(
    Output(component_id='national_individual_punting_table', component_property='data'),
    Output(component_id='national_individual_punting_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_punting_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                con.conference_shorthand as Conf,
                ps.games_played as GP,
                ps.punts as Punt,
                ps.punt_yards as Yds,
                ps.yards_per_punt as "Y/P"
            from punting_stat ps
            inner join roster r on ps.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by ps.punt_yards desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

@callback(
    Output(component_id='national_individual_scoring_title', component_property='children'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_scoring_title(selected_year):
    if not selected_year:
        raise PreventUpdate
    return f"{selected_year} National Scoring"

@callback(
    Output(component_id='national_individual_scoring_table', component_property='data'),
    Output(component_id='national_individual_scoring_table', component_property='columns'),
    Input(component_id='national_year_dropdown', component_property='value')
)
def update_national_individual_scoring_table(selected_year):
    if not selected_year:
        raise PreventUpdate
    query = f"""
            select
                p.player_name as Player,
                c.college_name as College,
                con.conference_shorthand as Conf,
                ss.games_played as GP,
                ss.rushing_touchdowns as RTD,
                ss.receiving_touchdowns as RecTD,
                ss.punt_return_touchdowns as PRTD,
                ss.kickoff_return_touchdowns as KRTD,
                ss.fumble_recovery_touchdowns as FRTD,
                ss.interception_return_touchdowns as ITD,
                ss.other_touchdowns as OTD,
                ss.total_touchdowns as TTD,
                ss.extra_points_made as XPM,
                ss.extra_points_attempted as XPA,
                ss.field_goals_made as FGM,
                ss.field_goals_attempted as FGA,
                ss.two_point_conversions_made as "2Pt",
                ss.safeties as SFY,
                ss.points_scored as Pts,
                ss.points_per_game as "P/G"
            from scoring_stat as ss
            inner join roster r on ss.roster_id = r.roster_id
            inner join player p on r.player_id = p.player_id
            inner join team t on r.team_id = t.team_id
            inner join college c on t.college_id = c.college_id
            inner join conference con on t.conference_id = con.conference_id
            where t.`year` = {selected_year}
            order by ss.points_scored desc;
            """
    df = load_data_query(query)
    columns = [{'name': col, 'id': col} for col in df.columns]
    return df.to_dict('records'), columns

app.run(debug=True, host='0.0.0.0', port=8050)