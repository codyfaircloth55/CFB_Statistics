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

def render_team_stats_tab():
    return [
        html.Div(
            [
                html.H3("Team Offense (Per Game Stats)")
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
                html.H3("Team Defense (Opponent Per Game Stats)")
            ]
        )
    ]

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
                    options = create_options(colleges, 'college_name'),
                    value = colleges['college_name'].unique()[0],
                    multi = False,
                    clearable = False,


                )
            ],
            style={"width": "48%", "display": "inline-block", "padding": "1%"}
        ),
        html.Div(
            [
                html.H2(id='college_page_title')
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
            ]
        ),
        dcc.Tabs(
            [
                dcc.Tab(render_team_stats_tab(), label="Team Stats")
            ]
        )
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
        ]
    ),
]

# Callbacks for by team tab
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
                toff.penalty_first_downs_per_game as PFD,
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

app.run(debug=True, host='0.0.0.0', port=8050)