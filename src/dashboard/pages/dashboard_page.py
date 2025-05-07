# dashboard/pages/dashboard_page.py

from dash import dcc, html
import plotly.express as px # For error placeholders

# Note: We assume generate_dashboard_figures and calculate_metrics
# have already been run in app.py and the results are passed here.

def create_layout(metrics, figures, grouped_keys):
    """
    Creates the layout for the aggregated dashboard page.

    Args:
        metrics (dict): Dictionary of calculated metrics.
        figures (dict): Dictionary mapping figure keys to Plotly figure objects.
        grouped_keys (OrderedDict): OrderedDict mapping tab labels to lists of figure keys.

    Returns:
        html.Div: The layout component for the dashboard page.
    """
    print("Creating dashboard layout...") # Add print statement for debugging

    # --- Create Metrics Cards ---
    # (Same logic as before in the old layout.py/app.py)
    metrics_cards = [
        html.Div(className='metric-card', children=[
            html.H3("Total Jobs Analyzed"),
            html.P(f"{metrics.get('total_jobs', 'N/A')}", id='db-metric-total-jobs') # Add prefix to IDs if needed
        ]),
        html.Div(className='metric-card', children=[
            html.H3("Unique Companies"),
            html.P(f"{metrics.get('unique_companies', 'N/A')}", id='db-metric-unique-companies')
        ]),
         html.Div(className='metric-card', children=[
            html.H3("Avg Min Experience"),
            html.P(f"{metrics.get('avg_min_exp', 'N/A')} Years", id='db-metric-avg-min-exp')
        ]),
        html.Div(className='metric-card', children=[
            html.H3("Most Common Role"),
            html.P(f"{metrics.get('common_role', 'N/A')}", id='db-metric-common-role')
        ]),
        html.Div(className='metric-card', children=[
            html.H3("Most Common Location"),
            html.P(f"{metrics.get('common_location', 'N/A')}", id='db-metric-common-location')
        ]),
        html.Div(className='metric-card', children=[
            html.H3("% Remote Friendly"),
            html.P(f"{metrics.get('remote_percentage', 'N/A')}%", id='db-metric-remote-percentage')
        ]),
    ]

    # --- Create Tabs and Content ---
    # (Same logic as before)
    tabs = []
    if not grouped_keys:
        tabs.append(dcc.Tab(label="No Data", value="tab-no-data", children=[
            html.P("No data available to display charts.", style={'textAlign': 'center', 'padding': '20px'})
        ]))
    else:
        for tab_label, fig_keys_in_tab in grouped_keys.items():
            tab_content = []
            for i in range(0, len(fig_keys_in_tab), 2):
                fig1_key = fig_keys_in_tab[i]
                fig2_key = fig_keys_in_tab[i+1] if i + 1 < len(fig_keys_in_tab) else None
                figure1 = figures.get(fig1_key, px.bar(title=f"{fig1_key} - Error"))
                figure2 = figures.get(fig2_key, px.bar(title=f"{fig2_key} - Error")) if fig2_key else None
                row_children = [
                    dcc.Graph(id=f'db-graph-{fig1_key}', figure=figure1, style={'minWidth': '400px', 'width': '48%', 'padding': '5px', 'flexGrow': 1})
                ]
                if figure2:
                     row_children.append(dcc.Graph(id=f'db-graph-{fig2_key}', figure=figure2, style={'minWidth': '400px', 'width': '48%', 'padding': '5px', 'flexGrow': 1}))
                else:
                     row_children.append(html.Div(style={'minWidth': '400px', 'width': '48%', 'padding': '5px', 'flexGrow': 1}))
                tab_content.append(
                    html.Div(className='chart-row-tab', children=row_children, style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around', 'gap': '10px', 'marginTop': '10px'})
                )
            tabs.append(
                dcc.Tab(label=tab_label, value=f'db-tab-{tab_label.lower().replace(" ","-")}', children=[
                    html.Div(tab_content, style={'padding': '15px'})
                ])
            )

    # --- Assemble Final Layout for this page ---
    layout = html.Div([
        html.P(f"Data from: {metrics.get('latest_ingestion_date', 'N/A')}",
               id='db-data-ingestion-date',
               style={'textAlign': 'center', 'marginBottom': '5px', 'color': 'grey', 'fontSize': '0.9em'}),
        html.Div(className='metrics-container', children=metrics_cards,
                 style={'display': 'flex', 'justifyContent': 'space-around', 'marginBottom': '20px', 'flexWrap': 'wrap'}),
        html.Hr(),
        html.H2("Aggregated Job Analysis", style={'textAlign': 'center', 'marginTop': '10px'}),
        dcc.Tabs(id="db-analysis-tabs",
                 value=f'db-tab-{list(grouped_keys.keys())[0].lower().replace(" ","-")}' if grouped_keys else 'tab-no-data',
                 children=tabs,
                 style={'marginTop': '10px'}),
    ])

    return layout