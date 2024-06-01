from dash import dcc, html


def single_data_viz(figure, title, description, section_id):
    """Generates a single data visualization section."""
    return [
        html.H2(title, className="heading-text", id=f"heading_text_{section_id}"),
        dcc.Graph(id=f'data-viz-chart-{section_id}', figure=figure, className='graph-bg-dark'),
        html.P(description, className="paragraph-text", id=f"paragraph_text_{section_id}_1"),
    ]

def data_viz(*visualizations):
    """Handles multiple visualizations. Each visualization is a tuple of (figure, title, description)."""
    content = []
    for idx, (figure, title, description) in enumerate(visualizations, start=1):
        content.extend(single_data_viz(figure, title, description, section_id=f'section{idx}'))
    return html.Div(content, className="section")

def generate(header_content, insights_content, *visualizations):
    return html.Div([
        # header_content,
        insights_content,
        data_viz(*visualizations)
    ], id='main-pane')
