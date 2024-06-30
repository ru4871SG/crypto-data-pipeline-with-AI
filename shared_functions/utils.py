## Utils.py
import dash
import dash_bootstrap_components as dbc
from dash import html

def format_title(words):
    if words == "Home":
        return words

    word = words.split()
    if len(word) > 0:
        word[0] = word[0].upper()
        word[1] = word[1].title()
    return " ".join(word)

def header():
    btc_pages = [page for page in dash.page_registry.values() if 'BTC' in page['title']]
    eth_pages = [page for page in dash.page_registry.values() if 'ETH' in page['title']]


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
                    href="https://twitter.com/",
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
                target="_blank"
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

# def update_dropdown(pathname):
#     pages = get_pages_based_on_url(pathname)
#     dropdown_items = [
#         dbc.DropdownMenuItem(
#             page['name'].split()[0],
#             children=[
#                 dbc.DropdownMenuItem(
#                     page['name'].replace(page['name'].split()[0] + ' ', ''),
#                     href=page["relative_path"]
#                 )
#             ]
#         ) for page in pages
#     ] if pages else []
#     return dropdown_items