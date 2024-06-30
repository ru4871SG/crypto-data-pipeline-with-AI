# %%
"""
Dash Page - ETH JUNE 2024 Report
"""

### Import Libraries
from app import header
from dotenv import load_dotenv
import shared_functions.main_pane as main_pane
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
collection_eth_1m_w_external = db.eth_1m_w_external_june_2024
collection_eth_1m_gas_fee = db.eth_1m_gas_fee_june_2024
collection_eth_1m_uniswap_data = db.eth_1m_uniswap_data_june_2024
collection_eth_1m_tvl = db.eth_1m_tvl_june_2024
collection_eth_1m_l2_bridge_all = db.eth_1m_l2_bridge_all_june_2024

eth_1m_w_external = pd.DataFrame(list(collection_eth_1m_w_external.find()))
eth_1m_gas_fee = pd.DataFrame(list(collection_eth_1m_gas_fee.find()))
eth_1m_uniswap_data = pd.DataFrame(list(collection_eth_1m_uniswap_data.find()))
eth_1m_tvl = pd.DataFrame(list(collection_eth_1m_tvl.find()))
eth_1m_l2_bridge_all = pd.DataFrame(list(collection_eth_1m_l2_bridge_all.find()))


# %%

### Section 1: Define the Plots Using Plotly
## fig1: Ethereum's Price Action Chart vs. Bitcoin (Normalized)
eth_1m_w_external.sort_values(by='target_block', inplace=True)
fig1_trace1 = go.Scatter(
    x=eth_1m_w_external['target_block'], 
    y=eth_1m_w_external['btc_price_normalized'], 
    mode='lines', 
    name='BTC', 
    line={'color': 'green'},
    hovertemplate='BTC Normalized: %{y}<extra></extra>'
)

fig1_trace2 = go.Scatter(
    x=eth_1m_w_external['target_block'], 
    y=eth_1m_w_external['eth_price_normalized'], 
    mode='lines', 
    name='ETH', 
    line={'color': 'blue'},
    hovertemplate='ETH Normalized: %{y}<extra></extra>'
)

index_values = list(range(1, len(eth_1m_w_external['target_block'])+1))

fig1_layout = go.Layout(
    height=400,
    xaxis=dict(
        tickvals=eth_1m_w_external['target_block'],
        ticktext=index_values,
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    title='ETH vs. BTC - Normalized',
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

fig1 = go.Figure(data=[fig1_trace1, fig1_trace2], layout=fig1_layout)

## fig2: Ethereum Gas Fees (GWEI)
eth_1m_gas_fee.sort_values(by='timestamp', inplace=True)

fig2 = px.bar(eth_1m_gas_fee, x="timestamp", y="gasPrice_close", \
              title="ETH Gas Fees (GWEI)", \
              color_discrete_sequence=['#a2823c'])

fig2.update_traces(hovertemplate="<b>Gas Price (Close):</b> %{y} GWEI")

fig2.update_layout(
    yaxis={
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white'},
        'titlefont': {'color': 'white'},
        'title': ''
    },
    xaxis={
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white'},
        'titlefont': {'color': 'white'},
        'showticklabels': False,
        'title': ''
    },
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}},
    title_font={'color': 'white'}
)

## fig3: Ethereum Uniswap Data
eth_1m_uniswap_data.sort_values(by='date', inplace=True)

fig3 = make_subplots(rows=2, cols=1)

fig3.add_trace(
    go.Scatter(
        x=eth_1m_uniswap_data['date'], 
        y=eth_1m_uniswap_data['tvlUSD'], 
        mode='lines', 
        name='tvlUSD', 
        line={'color': 'green'},
        hovertemplate='tvlUSD: %{y}<extra></extra>'
    ),
    row=1, col=1
)
fig3.add_trace(
    go.Scatter(
        x=eth_1m_uniswap_data['date'], 
        y=eth_1m_uniswap_data['volumeUSD'], 
        mode='lines', 
        name='volumeUSD', 
        line={'color': 'red'},
        hovertemplate='volumeUSD: %{y}<extra></extra>'
    ),
    row=2, col=1
)

fig3.update_layout(
    autosize=True,
    height=600,
    xaxis=dict(
        tickvals=eth_1m_uniswap_data['date'],
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    xaxis2=dict(
        tickvals=eth_1m_uniswap_data['date'],
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
    title='Ethereum Uniswap Data',
    titlefont={'color': 'white'},
    title_x=0.05,
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}},
    margin=dict(l=20, r=20, t=50, b=20),
)


## fig4: Ethereum Top DeFi TVL Historical Data Comparison (Minus Uniswap)
eth_1m_tvl.sort_values(by='row_index', inplace=True)

fig4_trace1 = go.Scatter(
    x=eth_1m_tvl['row_index'], 
    y=eth_1m_tvl['tvl_aave'], 
    mode='lines', 
    name='Aave', 
    line={'color': 'green'},
    hovertemplate='Aave TVL: %{y}<extra></extra>'
)

fig4_trace2 = go.Scatter(
    x=eth_1m_tvl['row_index'], 
    y=eth_1m_tvl['tvl_lido'], 
    mode='lines', 
    name='Lido', 
    line={'color': 'blue'},
    hovertemplate='Lido TVL: %{y}<extra></extra>'
)

fig4_trace3 = go.Scatter(
    x=eth_1m_tvl['row_index'], 
    y=eth_1m_tvl['tvl_makerdao'], 
    mode='lines', 
    name='MakerDao', 
    line={'color': 'yellow'},
    hovertemplate='MakerDao TVL: %{y}<extra></extra>'
)

index_values = list(range(1, len(eth_1m_tvl['row_index'])))

fig4_layout = go.Layout(
    height=400,
    xaxis=dict(
        tickvals=eth_1m_tvl['row_index'],
        ticktext=index_values,
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        autorange='reversed',
        showticklabels=False
    ),
    title='ETH TVL Comparison',
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

fig4 = go.Figure(data=[fig4_trace1, fig4_trace2, fig4_trace3], layout=fig4_layout)


## fig5: Ethereum L2 Bridge Data
eth_1m_l2_bridge_all.sort_values(by='day', inplace=True)

fig5 = go.Figure(data=[
    go.Bar(name='users_arbitrum', x=eth_1m_l2_bridge_all['day'], y=eth_1m_l2_bridge_all['users_arbitrum'], hovertemplate='users_arbitrum: %{y}<extra></extra>'),
    go.Bar(name='users_base', x=eth_1m_l2_bridge_all['day'], y=eth_1m_l2_bridge_all['users_base'], hovertemplate='users_base: %{y}<extra></extra>'),
    go.Bar(name='users_optimism', x=eth_1m_l2_bridge_all['day'], y=eth_1m_l2_bridge_all['users_optimism'], hovertemplate='users_optimism: %{y}<extra></extra>'),
    go.Bar(name='users_starknet', x=eth_1m_l2_bridge_all['day'], y=eth_1m_l2_bridge_all['users_starknet'], hovertemplate='users_starknet: %{y}<extra></extra>'),
    go.Bar(name='users_zksync', x=eth_1m_l2_bridge_all['day'], y=eth_1m_l2_bridge_all['users_zksync'], hovertemplate='users_zksync: %{y}<extra></extra>')
])

fig5.update_layout(barmode='stack')

fig5.update_layout(
    yaxis=dict(
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        titlefont={'color': 'white'},
        title=''
    ),
    xaxis=dict(
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        titlefont={'color': 'white'},
        showticklabels=False,
        title=''
    ),
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}},
    title_font={'color': 'white'}
)


# %%
### Section 2: Loading the Analysis Results from JSON File

# Load the analysis result from the JSON file
with open('./pages/ai_text_result/analysis_result_june_2024.json', 'r') as json_file:
    data = json.load(json_file)

# Access the value using the key
fig1_analysis_result = data['eth_1m_w_external']
fig2_analysis_result = data['eth_1m_gas_fee']
fig3_analysis_result = data['eth_1m_uniswap_data']
fig4_analysis_result = data['eth_1m_tvl']
fig5_analysis_result = data['eth_1m_l2_bridge_all']


# %%
### Section 3: Define the App Layout and Texts

def key_insights():
    return html.Div([
        html.H1("Ethereum (ETH) JUNE 2024 Report", className="title-text", id="title_text_1"),
    ])

# Inform Dash that this is a page
dash.register_page(__name__, title='ETH JUNE 2024 Report')

# Modify the default index string's title
index_string = '''
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <title>Ethereum JUNE 2024 Report</title>
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
        (fig1, "Ethereum Price Action vs. Bitcoin", fig1_analysis_result),

        (fig2, "Ethereum Gas Fees (GWEI)", fig2_analysis_result),

        (fig3, "Ethereum Uniswap Data", fig3_analysis_result),

        (fig4, "Ethereum TVL Comparison (without Uniswap)", fig4_analysis_result),

        (fig5, "Ethereum L2 Users", fig5_analysis_result)
    ),
    ], className='main'),
    html.Div([
        html.Img(src="assets/icons/ellipse.svg", alt=""),
    ],className="ellipse")
], id='main-container')
