import os
import datetime
from datetime import datetime as dt
from collections import defaultdict
import pandas as pd
import numpy as np
import sqlite3
import sqlalchemy

import dash
import dash_daq as daq
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import dash_table

import dash_bootstrap_components as dbc

import plotly.graph_objects as go
import plotly.express as px


#### Server Setup: ###
external_stylesheets = [dbc.themes.BOOTSTRAP]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title='Betterplace my money'
#Uncomment for production
# app = dash.Dash(__name__, external_stylesheets=external_stylesheets, requests_pathname_prefix='/benchmark_db/')
# app.url_base_pathname = '/benchmark_db/'
# app.routes_pathname_prefix = app.url_base_pathname
server = app.server
app.config.suppress_callback_exceptions = True

### Load Database ###
DATABASES = {
    'betterplace':{
        'NAME': os.environ['BETTERPLACE_DB_NAME'],
        'USER': os.environ['BETTERPLACE_DB_USER'],
        'PASSWORD': os.environ['BETTERPLACE_DB_PASSWORD'],
        'HOST': os.environ['BETTERPLACE_DB_HOST'],
        'PORT': os.environ['BETTERPLACE_DB_PORT'],
    },
}

# choose the database to use
db = DATABASES['betterplace']

# construct an engine connection string
engine_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
    user = db['USER'],
    password = db['PASSWORD'],
    host = db['HOST'],
    port = db['PORT'],
    database = db['NAME'],
)

# create sqlalchemy engine
conn = sqlalchemy.create_engine(engine_string)

### Loading Data
query_latest = """
Select carrier_name, city, country, donated_amount_in_cents, id, latitude, longitude, open_amount_in_cents, donations_count, progress_percentage, profile_picture, tags, title, summary, created_at, downloaded_at
FROM last_download_results"""
df_latest = pd.read_sql(query_latest, con=conn)
df_latest['donated_amount_in_euro'] = df_latest['donated_amount_in_cents']/100
df_latest['target_amount_in_euro'] = (df_latest['donated_amount_in_cents'] + df_latest['open_amount_in_cents'])/100
df_latest = df_latest.drop_duplicates(subset=["id"])

query_donations_id = """
Select id, downloaded_at, max(donated_amount_in_cents) as donated_amount_in_cents
FROM projects_vf
WHERE closed_at is NULL
GROUP BY id, downloaded_at"""
df_donations_id = pd.read_sql(query_donations_id, con=conn)
df_donations_id['donated_amount_in_euro'] = df_donations_id['donated_amount_in_cents']/100

query_donations_country = """
Select country, downloaded_at, sum(donated_amount_in_cents) as donated_amount_in_cents
FROM projects_vf
WHERE closed_at is NULL
GROUP BY country, downloaded_at
"""
df_donations_country = pd.read_sql(query_donations_country, con=conn)
df_donations_country['donated_amount_in_euro'] = df_donations_country ['donated_amount_in_cents']/100

query_donation_history_years = """
Select count(id) as projects, extract(year from created_at) as created_at_year
FROM all_projects_latest_download
group by created_at_year
"""
df_donation_history_years = pd.read_sql(query_donation_history_years, con=conn)
df_donation_history_years["type"]="historical"

### Create Cards ###
def description_card():
    """
    :return: A Div containing dashboard title & descriptions.
    """
    return html.Div(
        id="descriptions-card",
        children=[
            html.H5("Betterplace my money"),
            html.H3("Overview of current Betterplace projects"),
            dcc.Markdown('''
            This dashboard should give you a brief overview of ongoing [Betterplace](https://www.betterplace.org) projects and the donation behaviour of others. It is a small private project from me, as I was curious how donation behaviour changed due to the Covid situaction.

            *If you noticed some issues or like to contact me write a [mail](mailto:tobias-braun@gmx.net) or visit [Github](https://github.com/T0b145/Betterplace_Dash).*
            ''')
        ]
    )

def project_summary(df_latest):
    number_of_active_projects = df_latest.shape[0]
    avr_donation_size = int((df_latest.donated_amount_in_euro/df_latest.donations_count).replace([np.inf, -np.inf], np.nan).dropna().mean())

    mask = (df_latest['created_at'] > dt.now().replace(day=1 ,hour=1, minute=1, second=1, tzinfo=None))
    number_project_current_month = df_latest[mask].shape[0]
    mydate = datetime.datetime.now()
    month_name = mydate.strftime("%B")

    card_deck = dbc.CardDeck(
        [
            dbc.Card([
                dbc.CardHeader(html.B("Active Projects")),
                dbc.CardBody([daq.LEDDisplay(value=number_of_active_projects, color="black")], style={'textAlign': 'center'})
                ],className="mt-2 mr-1"),
            dbc.Card([
                dbc.CardHeader(html.B("Average donation size (€)")),
                dbc.CardBody([daq.LEDDisplay(value=avr_donation_size, color="black")], style={'textAlign': 'center'})
                ],className="mt-2 ml-1 mr-1"),
            dbc.Card([
                dbc.CardHeader(html.B("New projects in {}".format(month_name))),
                dbc.CardBody([daq.LEDDisplay(value=number_project_current_month, color="black")], style={'textAlign': 'center'})
                ],className="mt-2 ml-1")
        ]
    )
    return card_deck

def need_map(df_latest):
    title = "Location and received donations per project"
    value = 'target_amount_in_euro'
    px.set_mapbox_access_token("pk.eyJ1Ijoia2V2aW5oZWxkIiwiYSI6ImNrNnRreGxmYjAwYXAzZnBoMmFoYnNzNXQifQ.JtnVD89sYgpWVcIA0ZW3fQ")

    #df = df.round({'latitude': 0, 'longitude': 0})
    #df_group = df.groupby(["latitude", "longitude"], as_index=False).agg({value: 'sum', "country":"first"})
    #df_latest = df.query('country != "Deutschland"')

    fig = px.scatter_mapbox(df_latest,
    lat="latitude",
    lon="longitude",
    color="country",
    size=value,
    color_continuous_scale=px.colors.cyclical.IceFire,
    size_max=25,
    zoom=1,
    hover_name="title",
    hover_data = ["carrier_name"]
    )
    fig.update_layout(showlegend=False, margin= {"l":2,"r":2, "t":2,"b":2}, updatemenus= [{"type":"dropdown"}])
    return dbc.Card([
    dbc.CardHeader(html.B(title)),
    dbc.CardBody([dcc.Graph(figure=fig)])
    ],
    className="mt-2")

def target_per_country(df_donations_country):
    title = "History of the donations per country*"

    df_pivot = pd.pivot_table(df_donations_country,
        index='country',
        columns='downloaded_at',
        values= 'donated_amount_in_euro',
        aggfunc='sum')
    #df_pivot = df_pivot.dropna()
    df_pivot = df_pivot.sort_values(list(df_pivot)[len(list(df_pivot))-1], ascending=False)

    df_pivot_figure = df_pivot.drop(["Deutschland"])
    df_pivot_figure = df_pivot_figure.head(10)
    df_pivot_figure = df_pivot_figure.T

    fig = go.Figure()

    for i in range(len(list(df_pivot_figure))):
        fig.add_trace(
            go.Scatter(
                x=list(df_pivot_figure.index),
                y=df_pivot_figure.iloc[:,i],
                mode="lines+markers+text",
                text = [list(df_pivot_figure)[i]],
                textposition="top right",
                name= list(df_pivot_figure)[i]
            )
        )

    #fig.update_traces(textposition='top center')

    fig.update_layout(
        showlegend=False,
        yaxis_title="Donations (€)",
        xaxis_title="Measurement Date"
    )
    fig.update_layout(showlegend=False, margin= {"l":2,"r":2, "t":2,"b":2}, updatemenus= [{"type":"dropdown"}])

    return dbc.Card([
    dbc.CardHeader(html.B(title)),
    dbc.CardBody([dcc.Graph(figure=fig), "* Germany was excluded and only the top 10 countries are shown"])
    ],
    className="mt-2")

def donations_per_day(df_donations_id):
    title = "Average collected donations per day since last measurement"

    df_pivot = pd.pivot_table(df_donations_id,
        index='id',
        columns='downloaded_at',
        values= 'donated_amount_in_euro',
        aggfunc='first')
    df_pivot = df_pivot.dropna()

    total_downloads = len (list(df_pivot.sum().index))
    df_dif = pd.DataFrame(columns = ["Donations/day"])

    for i in range(total_downloads-1):
        dif = list(df_pivot.sum().index)[i+1]-list(df_pivot.sum().index)[i]
        days_dif = dif.days+ (dif.seconds/60/60/24)
        df_dif.loc[list(df_pivot.sum().index)[i+1]] = ((df_pivot.sum().iloc[i+1]- df_pivot.sum().iloc[i])/days_dif)

    fig = px.line(df_dif)
    fig.update_layout(
        showlegend=False,
        yaxis_title="Aver. daily donations",
        xaxis_title="Measurement Date")

    fig.update_layout(showlegend=False, margin= {"l":2,"r":2, "t":2,"b":2}, updatemenus= [{"type":"dropdown"}])

    return dbc.Card([
    dbc.CardHeader(html.B(title)),
    dbc.CardBody([dcc.Graph(figure=fig)])
    ],
    className="mt-2")

def Trending_Projects(df_donations_id, df_latest):
    df_details = df_latest.set_index("id")

    df_pivot = pd.pivot_table(df_donations_id,
    index='id',
    columns='downloaded_at',
    values= 'donated_amount_in_euro',
    aggfunc='first')
    df_pivot = df_pivot.dropna()

    no_columns = len(df_pivot.columns)
    time_delta = df_pivot.columns[no_columns-1]- df_pivot.columns[no_columns-2]
    time_delta = time_delta.days + (time_delta.seconds/60/60/24)

    df_trending = df_pivot.iloc[:,no_columns-1] - df_pivot.iloc[:,no_columns-2]
    df_trending = df_trending.sort_values(ascending=False)
    df_trending = df_trending.head(5)

    list_trends = []
    for id in list(df_trending.index):
        dict_trend = {
        "title" : df_details.at[id,"title"],
        "summary" : df_details.at[id,"summary"],
        "carrier_name" : df_details.at[id,"carrier_name"],
        "donated_amount_in_euro" : df_details.at[id,"donated_amount_in_euro"],
        "progress_percentage" : df_details.at[id,"progress_percentage"],
        "open_amount_in_cents" : df_details.at[id,"open_amount_in_cents"],
        "country" : df_details.at[id,"country"],
        "donations_per_day" : df_trending.loc[id]/time_delta
        }
        dict_trend["links"] = "https://www.betterplace.org/de/projects/"+str(id)
        profile_pictures = df_details.at[id,"profile_picture"]
        dict_trend["profile_picture"] = profile_pictures["links"][3]["href"]
        list_trends.append(dict_trend)

    Cards = []
    Cards.append(html.H4("Trending projects:"))

    for p in list_trends:
        Card = dbc.Card(
            [
                #dbc.CardHeader(html.B(p["title"], className="card-title")),
                dbc.CardImg(src=p["profile_picture"], top=True),
                dbc.CardBody(
                    [
                        html.B(html.A(p["title"], href=p["links"]), className="card-title"),
                        html.P(p["summary"], className="card-text"),
                    ]
                ),
                dbc.CardFooter([html.P("Carrier: {}".format(p["carrier_name"])),
                html.P("Collected Donations: {}€".format(int(p["donated_amount_in_euro"]))),
                html.P("Donations per day: {}€".format(int(p["donations_per_day"]))),
                daq.GraduatedBar(showCurrentValue=True, max=100, step=2,value=p["progress_percentage"])])
            ],
            style={"width": "100%"},
            className= "mb-3",
            outline=True,
            #color="dark",
            #inverse=True
        )
        Cards.append(Card)
    return html.Div(Cards)

def tag_popularity(df_latest):
    title = "Donations per Category of active projects (double counting possible)"
    dt_tags = defaultdict(int)

    for id in list(df_latest.index):
        tags = df_latest.at[id,"tags"]
        for tag in tags:
            if tag == "error" or tag =="Beliebteste":
                continue
            else:
                dt_tags[tag] += df_latest.at[id,"donated_amount_in_euro"]

    df_tag = pd.DataFrame.from_dict(dt_tags, orient='index', columns= ["donated_amount_in_euro"]).sort_values(by="donated_amount_in_euro", ascending=False)
    fig = px.bar(df_tag)
    fig.update_layout(
        showlegend=False,
        yaxis_title="Donations (€)",
        xaxis_title="Categories"
    )
    fig.update_layout(showlegend=False, margin= {"l":2,"r":2, "t":2,"b":2}, updatemenus= [{"type":"dropdown"}])

    return dbc.Card([
    dbc.CardHeader(html.B(title)),
    dbc.CardBody([dcc.Graph(figure=fig)])
    ],
    className="mt-2")

def donation_history_years(df_donation_history_years):
    title = "Number of the newly created projects per year"
    day_of_year = dt.now().timetuple().tm_yday
    last_year=df_donation_history_years.query('created_at_year=={}'.format(max(df_donation_history_years["created_at_year"])))
    fc_row = {"projects": last_year["projects"].values[0]/day_of_year*(365-day_of_year), "created_at_year":max(df_donation_history_years["created_at_year"]), "type":"forecast"}
    df_donation_history_years= df_donation_history_years.append(fc_row, ignore_index=True)

    fig = px.bar(df_donation_history_years, x="created_at_year", y="projects",color="type",labels={'created_at_year':'Year', 'projects': 'Number of new projects'})
    fig.update_layout(showlegend=False, margin= {"l":2,"r":2, "t":2,"b":2}, updatemenus= [{"type":"dropdown"}])

    return dbc.Card([
    dbc.CardHeader(html.B(title)),
    dbc.CardBody([dcc.Graph(figure=fig)])
    ],
    className="mt-2")

### Layout ###

app.layout = html.Div(
    id="app-container",
    children=[
        # Banner
        # html.Div(
        #     id="banner",
        #     className="banner",
        #     children=[html.Img(src=app.get_asset_url("plotly_logo.png"))],
        # ),
        # Left column
        dbc.Row([
            dbc.Col(
                html.Div(
                    id="left-column",
                    #className="four column",
                    children=[description_card(), html.Hr(),Trending_Projects(df_donations_id, df_latest)]
                    ),
                width = 4
            ),
            dbc.Col(
                html.Div(
                    id="right-column",
                    #className="eight column",
                    children=[project_summary(df_latest) ,need_map(df_latest), target_per_country(df_donations_country), donations_per_day(df_donations_id), tag_popularity(df_latest), donation_history_years(df_donation_history_years)]
                    ),
                width = 8
            )
            ]
        )
    ]
)

# Run the server
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8910)
