"""
Dash App
"""

# Import Libraries
import dash
import dash_bootstrap_components as dbc
from dash import Dash, html
from shared_functions.utils import header


# Use Multi-Pages
app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Deftify Monthly Cryptocurrency Analysis"

app.layout = html.Div([
    header(),
    dash.page_container
])

# Run the app for local testing
if __name__ == '__main__':
    app.run_server(debug=True, dev_tools_hot_reload=False)
