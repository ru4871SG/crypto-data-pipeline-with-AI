# %%
"""
Dash Page - Chainlink APRIL 2024 Report
"""

### Import Libraries
from app import header
import shared_functions.main_pane as main_pane

from dotenv import load_dotenv
import os
import json
import dash
from dash import html
from plotly.subplots import make_subplots

import pandas as pd

import plotly.express as px
import plotly.graph_objects as go

from pymongo import MongoClient

## Connect to environment variables
load_dotenv()
mongodb_uri = os.getenv('MONGODB_URI')

# Create a MongoDB client
client = MongoClient(mongodb_uri)
# Access the 'deftify_research' database
db = client.deftify_research
# Access the collections and load all documents into Pandas DataFrames
collection_chainlink_1m_w_external = db.chainlink_1m_w_external_april_2024
collection_chainlink_1m_uniswap_data = db.chainlink_1m_uniswap_data_april_2024
collection_chainlink_1m_aave_data = db.chainlink_1m_aave_data_april_2024

chainlink_1m_w_external = pd.DataFrame(list(collection_chainlink_1m_w_external.find()))
chainlink_1m_uniswap_data = pd.DataFrame(list(collection_chainlink_1m_uniswap_data.find()))
chainlink_1m_aave_data = pd.DataFrame(list(collection_chainlink_1m_aave_data.find()))


# %%

### Section 1: Define the Plots Using Plotly
## fig1: Chainlink's Price Action Chart vs. Bitcoin and ETH (Normalized)
chainlink_1m_w_external.sort_values(by='target_block', inplace=True)

fig1_trace1 = go.Scatter(
    x=chainlink_1m_w_external['target_block'], 
    y=chainlink_1m_w_external['btc_price_normalized'], 
    mode='lines', 
    name='BTC', 
    line={'color': 'yellow'},
    hovertemplate='BTC Normalized: %{y}<extra></extra>'
)

fig1_trace2 = go.Scatter(
    x=chainlink_1m_w_external['target_block'], 
    y=chainlink_1m_w_external['eth_price_normalized'], 
    mode='lines', 
    name='ETH', 
    line={'color': 'blue'},
    hovertemplate='ETH Normalized: %{y}<extra></extra>'
)

fig1_trace3 = go.Scatter(
    x=chainlink_1m_w_external['target_block'], 
    y=chainlink_1m_w_external['chainlink_price_normalized'], 
    mode='lines', 
    name='Link', 
    line={'color': 'green'},
    hovertemplate='Chainlink Normalized: %{y}<extra></extra>'
)

index_values = list(range(1, len(chainlink_1m_w_external['target_block'])+1))

fig1_layout = go.Layout(
    height=400,
    xaxis=dict(
        tickvals=chainlink_1m_w_external['target_block'],
        ticktext=index_values,
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    title='Chainlink vs. BTC and ETH - Normalized',
    titlefont={'color': 'white'},
    title_x=0.05,
    yaxis=dict(
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        titlefont={'color': 'white'},
        title=''
    ),
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}}
)

fig1 = go.Figure(data=[fig1_trace1, fig1_trace2, fig1_trace3], layout=fig1_layout)


## fig2: Chainlink Uniswap Data
chainlink_1m_uniswap_data.sort_values(by='target_block', inplace=True)

fig2 = make_subplots(rows=2, cols=1)

fig2.add_trace(
    go.Scatter(
        x=chainlink_1m_uniswap_data['target_block'], 
        y=chainlink_1m_uniswap_data['totalValueLockedUSD'], 
        mode='lines', 
        name='tvlUSD', 
        line={'color': 'green'},
        hovertemplate='tvlUSD: %{y}<extra></extra>'
    ),
    row=1, col=1
)
fig2.add_trace(
    go.Scatter(
        x=chainlink_1m_uniswap_data['target_block'], 
        y=chainlink_1m_uniswap_data['dailyVolumeUSD'], 
        mode='lines', 
        name='dailyVolumeUSD', 
        line={'color': 'orange'},
        hovertemplate='dailyVolumeUSD: %{y}<extra></extra>'
    ),
    row=2, col=1
)

fig2.update_layout(
    autosize=True,
    height=600,
    xaxis=dict(
        tickvals=chainlink_1m_uniswap_data['target_block'],
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    xaxis2=dict(
        tickvals=chainlink_1m_uniswap_data['target_block'],
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    yaxis=dict(
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        titlefont={'color': 'white'},
        title=''
    ),
    yaxis2=dict(
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        titlefont={'color': 'white'},
        title=''
    ),
    title='Chainlink Uniswap Data',
    titlefont={'color': 'white'},
    title_x=0.05,
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}},
    margin=dict(l=20, r=20, t=50, b=20),
)


## fig3: Chainlink Aave Data
chainlink_1m_aave_data.sort_values(by='target_block', inplace=True)

fig3 = make_subplots(rows=3, cols=1)

fig3.add_trace(
    go.Scatter(
        x=chainlink_1m_aave_data['target_block'], 
        y=chainlink_1m_aave_data['totalValueLockedUSD'], 
        mode='lines', 
        name='tvlUSD', 
        line={'color': 'green'},
        hovertemplate='tvlUSD: %{y}<extra></extra>'
    ),
    row=1, col=1
)
fig3.add_trace(
    go.Scatter(
        x=chainlink_1m_aave_data['target_block'], 
        y=chainlink_1m_aave_data['totalBorrowBalanceUSD'], 
        mode='lines', 
        name='totalBorrowBalanceUSD', 
        line={'color': 'blue'},
        hovertemplate='totalBorrowBalanceUSD: %{y}<extra></extra>'
    ),
    row=2, col=1
)
fig3.add_trace(
    go.Scatter(
        x=chainlink_1m_aave_data['target_block'], 
        y=chainlink_1m_aave_data['totalDepositBalanceUSD'], 
        mode='lines', 
        name='totalDepositBalanceUSD', 
        line={'color': 'red'},
        hovertemplate='totalDepositBalanceUSD: %{y}<extra></extra>'
    ),
    row=3, col=1
)

fig3.update_layout(
    autosize=True,
    height=600,
    xaxis=dict(
        tickvals=chainlink_1m_aave_data['target_block'],
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    xaxis2=dict(
        tickvals=chainlink_1m_aave_data['target_block'],
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    xaxis3=dict(
        tickvals=chainlink_1m_aave_data['target_block'],
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    yaxis=dict(
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        titlefont={'color': 'white'},
        title=''
    ),
    yaxis2=dict(
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        titlefont={'color': 'white'},
        title=''
    ),
    yaxis3=dict(
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        titlefont={'color': 'white'},
        title=''
    ),
    title='Chainlink Aave Data',
    titlefont={'color': 'white'},
    title_x=0.05,
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}},
    margin=dict(l=20, r=20, t=50, b=20),
)


# %%
### Section 2: Loading the Analysis Results from JSON File

# Load the analysis result from the JSON file
with open('./pages/ai_text_result/analysis_result_april_2024.json', 'r') as json_file:
    data = json.load(json_file)

# Access the value using the key
fig1_analysis_result = data['chainlink_1m_w_external']
fig2_analysis_result = data['chainlink_1m_uniswap_data']
fig3_analysis_result = data['chainlink_1m_aave_data']


# %%
### Section 3: Define the App Layout and Texts

def key_insights():
    return html.Div([
        html.H1("Chainlink (LINK) APRIL 2024 Report", className="title-text", id="title_text_1"),
    ])

# Inform Dash that this is a page
dash.register_page(__name__, title='Chainlink APRIL 2024 Report')

# Modify the default index string's title
index_string = '''
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <title>Chainlink APRIL 2024 Report</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Define the app layout for this page
layout = html.Div([
    html.Div([

    main_pane.generate(
        header(),
        key_insights(),
        (fig1, "Chainlink Price Action vs. BTC and ETH", fig1_analysis_result),

        (fig2, "Chainlink Uniswap Data", fig2_analysis_result),

        (fig3, "Chainlink AAVE Data", fig3_analysis_result)
    ),
    ], className='main'),
    html.Div([
        html.Img(src="assets/icons/ellipse.svg", alt=""),
    ],className="ellipse")
], id='main-container')
