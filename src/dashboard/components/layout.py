# dashboard/components/layout.py

from dash import dcc, html
import plotly.express as px # Needed for placeholder error figures

def create_layout(metrics, figures, grouped_keys):
    """
    Creates the Dash application layout.

    Args:
        metrics (dict): Dictionary of calculated metrics.
        figures (dict): Dictionary mapping figure keys to Plotly figure objects.
        grouped_keys (OrderedDict): OrderedDict mapping tab labels to lists of figure keys.

    Returns:
        html.Div: The main layout component for the Dash app.
    """

    # --- Create Metrics Cards ---
    metrics_cards = [
        html.Div(className='metric-card', children=[
            html.H3("Total Jobs Analyzed"),
            html.P(f"{metrics.get('total_jobs', 'N/A')}", id='metric-total-jobs')
        ]),
        html.Div(className='metric-card', children=[
            html.H3("Unique Companies"),
            html.P(f"{metrics.get('unique_companies', 'N/A')}", id='metric-unique-companies')
        ]),
         html.Div(className='metric-card', children=[
            html.H3("Avg Min Experience"),
            html.P(f"{metrics.get('avg_min_exp', 'N/A')} Years", id='metric-avg-min-exp')
        ]),
        html.Div(className='metric-card', children=[
            html.H3("Most Common Role"),
            html.P(f"{metrics.get('common_role', 'N/A')}", id='metric-common-role')
        ]),
        html.Div(className='metric-card', children=[
            html.H3("Most Common Location"),
            html.P(f"{metrics.get('common_location', 'N/A')}", id='metric-common-location')
        ]),
        html.Div(className='metric-card', children=[
            html.H3("% Remote Friendly"),
            html.P(f"{metrics.get('remote_percentage', 'N/A')}%", id='metric-remote-percentage')
        ]),
    ]

    # --- Create Tabs and Content ---
    tabs = []
    if not grouped_keys: # Handle case where no figures/groups were generated
        tabs.append(dcc.Tab(label="No Data", value="tab-no-data", children=[
            html.P("No data available to display.", style={'textAlign': 'center', 'padding': '20px'})
        ]))
    else:
        for tab_label, fig_keys_in_tab in grouped_keys.items():
            tab_content = []
            for i in range(0, len(fig_keys_in_tab), 2):
                fig1_key = fig_keys_in_tab[i]
                fig2_key = fig_keys_in_tab[i+1] if i + 1 < len(fig_keys_in_tab) else None

                # Get figures, provide placeholder on error
                figure1 = figures.get(fig1_key, px.bar(title=f"{fig1_key} - Error").update_layout(annotations=[dict(text="Figure Error", showarrow=False)]))
                figure2 = figures.get(fig2_key, px.bar(title=f"{fig2_key} - Error").update_layout(annotations=[dict(text="Figure Error", showarrow=False)])) if fig2_key else None

                row_children = [
                    dcc.Graph(
                        id=f'graph-{fig1_key}',
                        figure=figure1,
                        style={'minWidth': '400px', 'width': '48%', 'padding': '5px', 'flexGrow': 1}
                    )
                ]
                if figure2:
                     row_children.append(dcc.Graph(
                        id=f'graph-{fig2_key}',
                        figure=figure2,
                        style={'minWidth': '400px', 'width': '48%', 'padding': '5px', 'flexGrow': 1}
                    ))
                else:
                     # Add an empty div to maintain alignment if only one graph in the last row
                     row_children.append(html.Div(style={'minWidth': '400px', 'width': '48%', 'padding': '5px', 'flexGrow': 1}))

                tab_content.append(
                    html.Div(className='chart-row-tab', children=row_children,
                             style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around', 'gap': '10px', 'marginTop': '10px'})
                )

            tabs.append(
                dcc.Tab(label=tab_label, value=f'tab-{tab_label.lower().replace(" ","-")}', children=[
                    html.Div(tab_content, style={'padding': '15px'}) # Padding for tab content
                ])
            )

    # --- Assemble Final Layout ---
    layout = html.Div(children=[
        html.H1(children='LinkedIn Job Data Dashboard', style={'textAlign': 'center'}),
        html.P(f"Showing data for latest ingestion: {metrics.get('latest_ingestion_date', 'N/A')}",
            id='data-ingestion-date',
            style={'textAlign': 'center', 'marginBottom': '5px'}),

        # Metrics Section
        html.Div(className='metrics-container', children=metrics_cards,
                 style={'display': 'flex', 'justifyContent': 'space-around', 'marginBottom': '20px', 'flexWrap': 'wrap'}),

        html.Hr(),

        # Tabs Section
        html.H2("Job Data Analysis", style={'textAlign': 'center', 'marginTop': '10px'}),
        dcc.Tabs(id="analysis-tabs",
                 value=f'tab-{list(grouped_keys.keys())[0].lower().replace(" ","-")}' if grouped_keys else 'tab-no-data', # Default to first tab or 'no-data'
                 children=tabs,
                 style={'marginTop': '10px'}),

        html.Footer("Data sourced from BigQuery", style={'marginTop': '40px', 'textAlign': 'center', 'color': 'grey'})

    ], style={'padding': '15px'}) # Overall page padding

    return layout