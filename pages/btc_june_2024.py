# %%
"""
Dash Page - BTC JUNE 2024 Report
"""

### Import Libraries
from app import header
import shared_functions.main_pane as main_pane

from dotenv import load_dotenv
import os
import json
import dash
from dash import html

import pandas as pd

from plotly.subplots import make_subplots
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
collection_btc_1m_w_external = db.btc_1m_w_external_june_2024
collection_btc_1m_mempool_fee = db.btc_1m_mempool_fee_june_2024
collection_btc_1m_mempool_price = db.btc_1m_mempool_price_june_2024
collection_btc_1m_mempool_hashrate = db.btc_1m_mempool_hashrate_june_2024
collection_btc_mempool_mining_pools = db.btc_mempool_mining_pools_june_2024
collection_btc_1m_mempool_lightning = db.btc_1m_mempool_lightning_june_2024
collection_btc_1m_dune_fee_breakdown = db.btc_1m_dune_fee_breakdown_june_2024

btc_1m_w_external = pd.DataFrame(list(collection_btc_1m_w_external.find()))
btc_1m_mempool_fee = pd.DataFrame(list(collection_btc_1m_mempool_fee.find()))
btc_1m_mempool_price = pd.DataFrame(list(collection_btc_1m_mempool_price.find()))
btc_1m_mempool_hashrate = pd.DataFrame(list(collection_btc_1m_mempool_hashrate.find()))
btc_mempool_mining_pools = pd.DataFrame(list(collection_btc_mempool_mining_pools.find()))
btc_1m_mempool_lightning = pd.DataFrame(list(collection_btc_1m_mempool_lightning.find()))
btc_1m_dune_fee_breakdown = pd.DataFrame(list(collection_btc_1m_dune_fee_breakdown.find()))



# %%

### Section 1: Define the Plots Using Plotly
## fig1: Bitcoin's Price Action Chart vs. Gold (Normalized)
btc_1m_w_external.sort_values(by='target_block', inplace=True)

fig1_trace1 = go.Scatter(
    x=btc_1m_w_external['target_block'], 
    y=btc_1m_w_external['btc_price_normalized'], 
    mode='lines', 
    name='BTC', 
    line={'color': 'green'},
    hovertemplate='BTC Normalized: %{y}<extra></extra>'
)

fig1_trace2 = go.Scatter(
    x=btc_1m_w_external['target_block'], 
    y=btc_1m_w_external['gold_price_normalized'], 
    mode='lines', 
    name='Gold', 
    line={'color': 'gold'},
    hovertemplate='Gold Normalized: %{y}<extra></extra>'
)

index_values = list(range(1, len(btc_1m_w_external['target_block'])+1))

fig1_layout = go.Layout(
    height=400,
    xaxis=dict(
        tickvals=btc_1m_w_external['target_block'],
        ticktext=index_values,
        gridcolor='#636363',
        zerolinecolor='#636363',
        tickfont={'color': 'white'},
        showticklabels=False
    ),
    title='BTC vs. Gold - Normalized',
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

## fig2: Scatter Plot Between BTC vs. Gold (Normalized)
fig2 = px.scatter(btc_1m_w_external, x='btc_price_normalized',
                  y='gold_price_normalized',
                  labels={'btc_price_normalized': 'BTC',
                  'gold_price_normalized': 'Gold'}, 
                  title='BTC vs. Gold')

fig2_layout = fig2.layout

fig2_layout.update(
    height=400,
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    yaxis={
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white'},
        'titlefont': {'color': 'white'}
    },
    xaxis={
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white'},
        'titlefont': {'color': 'white'}
    },
    legend={'font': {'color': 'white'}},
    title_font={'color': 'white'}
)

fig2 = {"data": fig2.data, "layout": fig2_layout}

## fig3: Line Chart Showing BTC Hashrate and Price
# Extract date from 'time' column in 'btc_1m_mempool_hashrate'
btc_1m_mempool_hashrate['date_for_join'] = btc_1m_mempool_hashrate['time'].dt.date
btc_1m_mempool_price['date_for_join'] = btc_1m_mempool_price['date'].dt.date
# Merge 'btc_1m_mempool_hashrate' and 'btc_1m_mempool_price' on 'date_for_join'
btc_1m_mempool_hashrate_vs_price = pd.merge(btc_1m_mempool_hashrate, btc_1m_mempool_price, on='date_for_join', how='inner')

btc_1m_mempool_hashrate_vs_price.sort_values(by='date_for_join', inplace=True)

fig3 = make_subplots(rows=2, cols=1)
fig3.add_trace(
    go.Scatter(
        x=btc_1m_mempool_hashrate_vs_price['date_for_join'],
        y=btc_1m_mempool_hashrate_vs_price['USD'],
        mode='lines',
        name='USD',
        line={'color': 'green'},
        hovertemplate='Price: %{y}<extra></extra>'
    ),
    row=1, col=1
)

fig3.add_trace(
    go.Scatter(
        x=btc_1m_mempool_hashrate_vs_price['date_for_join'],
        y=btc_1m_mempool_hashrate_vs_price['avgHashrate_EHs'],
        mode='lines',
        name='Hashrate (EH/s)',
        line={'color': 'red'},
        hovertemplate='Hashrate (EH/s): %{y}<extra></extra>'
    ),
    row=2, col=1
)

# Update yaxis properties
fig3.update_yaxes(title_text="BTC Price", row=1, col=1, gridcolor='#636363', zerolinecolor='#636363', tickfont={'color': 'white'}, titlefont={'color': 'white'})
fig3.update_yaxes(title_text="BTC Hashrate (EH/s)", row=2, col=1, gridcolor='#636363', zerolinecolor='#636363', tickfont={'color': 'white'}, titlefont={'color': 'white'})

# Update xaxis properties
fig3.update_xaxes(title_text="Date", row=1, col=1, gridcolor='#636363', zerolinecolor='#636363', tickfont={'color': 'white'}, titlefont={'color': 'white'}, showticklabels=False)
fig3.update_xaxes(title_text="Date", row=2, col=1, gridcolor='#636363', zerolinecolor='#636363', tickfont={'color': 'white'}, titlefont={'color': 'white'}, showticklabels=False)

# Layout adjustments
fig3.update_layout(
    autosize=True,
    height=600,
    title_text="BTC Price and Hashrate",
    titlefont={'color': 'white'},
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}},
    margin=dict(l=20, r=20, t=50, b=20),
)


## fig4: Bitcoin Median Tx Fee Over Time
# Convert 'time' to datetime and extract the date
btc_1m_mempool_fee['time'] = pd.to_datetime(btc_1m_mempool_fee['time'])
btc_1m_mempool_fee['date'] = btc_1m_mempool_fee['time'].dt.date
# Group by 'date' and calculate the average of 'avgFee_50'
btc_1m_mempool_fee_avg = btc_1m_mempool_fee.groupby('date')['avgFee_50'].mean().reset_index()

btc_1m_mempool_fee_avg.sort_values(by='date', inplace=True)

fig4 = px.bar(btc_1m_mempool_fee_avg, x="date", y="avgFee_50", \
              title="Bitcoin Median Tx Fee Over Time", \
              color_discrete_sequence=['#a2823c'], \
              custom_data=['date', 'avgFee_50'])

fig4.update_traces(hovertemplate="<b>Median Fee:</b> %{customdata[1]:.0f} sat/vB")

fig4.update_layout(
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


## fig5: Bitcoin Mining Pools
top_10_pools = btc_mempool_mining_pools.sort_values(by='blockCount', ascending=False).head(10)
# Group the outside top 10 mining pools as "Others"
other_pools = btc_mempool_mining_pools[~btc_mempool_mining_pools['name'].isin(top_10_pools['name'])]
other_block_count = other_pools['blockCount'].sum()
other_pools = pd.DataFrame({'name': ['Others'], 'blockCount': [other_block_count]})

pools = pd.concat([top_10_pools, other_pools])

# Calculate the percentage of block count for each pool
pools['percentage'] = pools['blockCount'] / pools['blockCount'].sum() * 100

fig5 = px.pie(pools, values='blockCount', names='name', title='Bitcoin Mining Pools',\
              hover_data=['percentage'])

fig5.update_traces(
    hovertemplate='<b>%{label}</b><br>' +
                  'blockCount: %{value}<br>' +
                  'Percentage: %{customdata[0]:.2f}%<extra></extra>',
    textposition='outside',
    textinfo='percent+label',
    insidetextorientation='radial',
    textfont={'color': 'white'}
)

fig5.update_layout(
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


## fig6: Lightning Network Stats
btc_1m_mempool_lightning.sort_values(by='added', inplace=True)

trace1 = go.Scatter(x=btc_1m_mempool_lightning['added'],
                    y=btc_1m_mempool_lightning['total_capacity'],
                    mode='lines',
                    name='Total Capacity',
                    yaxis='y1',
                    text=['Total Capacity: {:.0f}'.format(val) \
                          for val in btc_1m_mempool_lightning['total_capacity']],
                    hoverinfo='text+y',
                    hovertemplate='<b>%{text}</b>')

trace2 = go.Scatter(x=btc_1m_mempool_lightning['added'],
                    y=btc_1m_mempool_lightning['channel_count'],
                    mode='lines',
                    name='Channel Count',
                    yaxis='y2',
                    text=['Channel Count: {:.0f}'.format(val) \
                          for val in btc_1m_mempool_lightning['channel_count']],
                    hoverinfo='text+y',
                    hovertemplate='<b>%{text}</b>')

layout_fig6 = go.Layout(
    title='Mempool Stats - Lightning Network',
    title_font={'color': 'white'},
    xaxis={
        'title': 'Date',
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white', 'size': 10},
        'titlefont': {'color': 'white'},
        'showticklabels': False,
        'title': ''
    },
    yaxis={
        'title': 'Total Capacity',
        'side': 'left',
        'showgrid': False,
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white', 'size': 8},
        'titlefont': {'color': 'white'}
    },
    yaxis2={
        'title': 'Channel Count',
        'side': 'right',
        'overlaying': 'y',
        'showgrid': False,
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white', 'size': 8},
        'titlefont': {'color': 'white'}
    },
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}}
)

fig6 = go.Figure(data=[trace1, trace2], layout=layout_fig6)

## fig7: Bitcoin Fee Breakdown from Dune Analytics
btc_1m_dune_fee_breakdown.sort_values(by='Day', inplace=True)

fig7 = go.Figure(data=[
    go.Bar(name='BRC20 Tx', x=btc_1m_dune_fee_breakdown['Day'], y=btc_1m_dune_fee_breakdown['BRC20_Tx'], hovertemplate='BRC20 Tx: %{y}<extra></extra>'),
    go.Bar(name='Non-BRC20 Tx', x=btc_1m_dune_fee_breakdown['Day'], y=btc_1m_dune_fee_breakdown['non_BRC20_Ordi_Tx'], hovertemplate='Non-BRC20 Ordinals Tx: %{y}<extra></extra>'),
    go.Bar(name='Non-Ordinal Tx', x=btc_1m_dune_fee_breakdown['Day'], y=btc_1m_dune_fee_breakdown['non_Odrdinal_Tx'], hovertemplate='Non-Ordinals Tx: %{y}<extra></extra>')
])

fig7.update_layout(
    barmode='stack',
    title='Bitcoin Fee Breakdown',
    title_font={'color': 'white'},
    xaxis={
        'title': 'Date',
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white', 'size': 10},
        'titlefont': {'color': 'white'},
        'showticklabels': False,
        'title': ''
    },
    yaxis={
        'title': 'Transaction Count',
        'side': 'left',
        'showgrid': False,
        'gridcolor': '#636363',
        'zerolinecolor': '#636363',
        'tickfont': {'color': 'white', 'size': 8},
        'titlefont': {'color': 'white'}
    },
    plot_bgcolor='#171713',
    paper_bgcolor='#171713',
    legend={'font': {'color': 'white'}}
)

# %%
### Section 2: Loading the Analysis Results from JSON File

# Load the analysis result from the JSON file
with open('./pages/ai_text_result/analysis_result_june_2024.json', 'r') as json_file:
    data = json.load(json_file)

# Access the value using the key
fig1_analysis_result = data['btc_1m_w_external']
fig2_analysis_result = data['btc_1m_w_external_correlation']
fig3_analysis_result = data['btc_1m_mempool_hashrate']
fig4_analysis_result = data['btc_1m_mempool_fee']
fig5_analysis_result = data['btc_mempool_mining_pools']
fig6_analysis_result = data['btc_1m_mempool_lightning']
fig7_analysis_result = data['btc_1m_dune_fee_breakdown']


# %%
### Section 3: Define the App Layout and Texts

def key_insights():
    return html.Div([
        html.H1("Bitcoin (BTC) JUNE 2024 Report", className="title-text", id="title_text_1"),
    ])

# Inform Dash that this is a page
dash.register_page(__name__, title='BTC JUNE 2024 Report')

# Modify the default index string's title
index_string = '''
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8">
        <title>Bitcoin JUNE 2024 Report</title>
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
        (fig1, "Bitcoin Price Action vs. Gold", fig1_analysis_result),

        (fig2, "Correlation Analysis: Bitcoin vs. Gold", fig2_analysis_result),

        (fig3, "Bitcoin Price and Hashrate", fig3_analysis_result),

        (fig4, "Bitcoin Block Analysis - Median Tx Fee", fig4_analysis_result),

        (fig5, "Bitcoin Mining Pools", fig5_analysis_result),

        (fig6, "Lightning Network (LN) Data", fig6_analysis_result), 

        (fig7, "Bitcoin Fee Breakdown", fig7_analysis_result)
    ),
    ], className='main'),
    html.Div([
        html.Img(src="assets/icons/ellipse.svg", alt=""),
    ],className="ellipse")
], id='main-container')
