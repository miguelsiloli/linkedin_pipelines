# dashboard/app.py

import dash
from dash import Dash, dcc, html, Input, Output, State

# --- Relative Imports ---
# Data Layer
from .bq_connector import fetch_latest_job_data
# Utils
from .utils.processing import calculate_metrics
# Components
from .components.figures import generate_dashboard_figures
from .components.layout_components import create_navbar # Import Navbar
# Pages MUST be imported so their callbacks are registered
from .pages import dashboard_page, job_listings_page

# --- Initialize Dash App ---
# Add suppress_callback_exceptions=True if callbacks are in different files
# Using external_stylesheets can be good for production styling
app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server
app.title = "LinkedIn Job Dashboard"

# --- Load Data ONCE ---
# This df_loaded is now potentially accessible by callbacks imported later
print("Attempting to fetch data from BigQuery...")
df_loaded, latest_date_str = fetch_latest_job_data()

# --- Pre-calculate Metrics and Figures for Dashboard ---
# These are needed only if the dashboard page is loaded
print("Calculating metrics for dashboard...")
calculated_metrics = calculate_metrics(df_loaded)
if latest_date_str: # Use date from connector if available
     calculated_metrics['latest_ingestion_date'] = latest_date_str
print(f"Metrics calculated. Latest Date: {calculated_metrics['latest_ingestion_date']}")

print("Generating figures for dashboard...")
figures_dict, grouped_keys = generate_dashboard_figures(df_loaded)
print("Figures generated.")

# --- Define App Layout (Core Structure) ---
# Includes Navbar, Location, and Page Content container
app.layout = html.Div([
    dcc.Location(id='url', refresh=False), # Tracks URL
    create_navbar(),                      # Shared navigation
    html.Div(id='page-content', style={'marginTop': '10px'}) # Content area
])

# --- Routing Callback ---
@app.callback(
    Output('page-content', 'children'),
    Input('url', 'pathname')
)
def display_page(pathname):
    """Renders the correct page layout based on the URL pathname."""
    print(f"Routing callback triggered. Pathname: {pathname}") # Debug print
    if pathname is None:
        # Handle initial load or empty pathname
        print("Routing: No pathname, defaulting to dashboard.")
        return dashboard_page.create_layout(calculated_metrics, figures_dict, grouped_keys)

    if pathname.startswith('/jobs'):
        # For /jobs or /jobs/job_id, show the listings layout
        # The callback within job_listings_page handles the details pane update
        print("Routing: Path starts with /jobs, loading job listings page.")
        return job_listings_page.create_layout(df_loaded) # Pass the loaded data
    elif pathname == '/':
        # Show the aggregated dashboard
        print("Routing: Path is /, loading dashboard page.")
        return dashboard_page.create_layout(calculated_metrics, figures_dict, grouped_keys)
    else:
        # Handle unknown paths
        print(f"Routing: Unknown path {pathname}, showing 404.")
        return html.Div([
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ])

# --- Run the App ---
if __name__ == '__main__':
    # Use host='0.0.0.0' to make it accessible on your network
    # Set debug=False for production
    app.run(debug=True, host='0.0.0.0', port=8050)