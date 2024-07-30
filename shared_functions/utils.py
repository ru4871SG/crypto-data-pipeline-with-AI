# Import Libraries
from dash import html
import dash
import dash_bootstrap_components as dbc

from datetime import datetime
import re

# Function to handle the title format
def format_title(words):
    if words == "Home":
        return words

    word = words.split()
    if len(word) > 0:
        word[0] = word[0].upper()
        word[1] = word[1].title()
    return " ".join(word)

# Function to extract the date
def extract_date(title):
    match = re.search(r'(\w+ \d{4})', title)
    if match:
        return datetime.strptime(match.group(1), '%B %Y')
    return datetime.min

# Function for the header
def header():
    btc_pages = sorted(
        [page for page in dash.page_registry.values() if 'BTC' in page['title']],
        key=lambda page: extract_date(page['title']),
        reverse=True
    )
    eth_pages = sorted(
        [page for page in dash.page_registry.values() if 'ETH' in page['title']],
        key=lambda page: extract_date(page['title']),
        reverse=True
    )

    return html.Div([
        html.Div([
            html.A(
                html.Img(src="assets/icons/icon-deftify.svg", alt="Deftify"),
                href="/"
            ),
            html.Div([
                html.Div([
                    html.A(
                        html.Img(src="assets/icons/icon-twitter.svg", alt="Deftify"),
                        href="https://x.com/",
                        className="socials_link",
                        target='_blank'
                    ),
                    html.A(
                        html.Img(src="assets/icons/icon-medium.svg", alt="Deftify"),
                        href="https://medium.com/",
                        className="socials_link",
                        target='_blank'
                    ),
                    html.A(
                        html.Img(src="assets/icons/icon-telegram.svg", alt="Deftify"),
                        href="https://telegram.org/",
                        className="socials_link",
                        target='_blank'
                    ),
                ], className="header_socials_row"),
                html.A(
                    html.H4("Deftify Beta"),
                    href="#",
                    className="button_link",
                    target="_self"
                ),
            ], className="header_small_row")
        ], className='header_details'),
        html.Div([
            html.Div([
                dbc.DropdownMenu(
                    label=html.Div([
                        html.Div([
                            html.Img(src="assets/icons/btc.png", alt="btc"),
                        ], className="link_img"),
                        html.H4('Bitcoin')
                    ], className="header_small_row"),
                    children=[
                        dbc.DropdownMenuItem(
                            format_title(page['name'].replace('BTC ', '')),
                            href=page["relative_path"],
                            ) for page in btc_pages
                    ],
                    className="page_link",
                ),
                dbc.DropdownMenu(
                    label=html.Div([
                        html.Div([
                            html.Img(src="assets/icons/eth.png", alt="btc"),
                        ], className="link_img"),
                        html.H4('Ethereum')
                    ], className="header_small_row"),
                    children=[
                        dbc.DropdownMenuItem(
                            format_title(page['name'].replace('ETH ', '')),
                            href=page["relative_path"],
                            ) for page in eth_pages
                    ],
                    className="page_link",
                ),
            ], className="header_small_row"),
        ], className="header_links")
    ], className='header')
