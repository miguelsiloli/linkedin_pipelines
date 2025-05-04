# dashboard/app.py

import dash
from dash import Dash
import pandas as pd

# --- Relative Imports ---
# Data Layer
from bq_connector import fetch_latest_job_data
# Utils
from .utils.processing import calculate_metrics
# Components
from .components.figures import generate_dashboard_figures
from .components.layout import create_layout

# --- Initialize Dash App ---
# Consider adding external_stylesheets=[dbc.themes.BOOTSTRAP] etc. if using Dash Bootstrap
app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server
app.title = "LinkedIn Job Dashboard" # Set browser tab title

# --- Load Data ---
print("Attempting to fetch data from BigQuery...")
# df_loaded can be None if connector fails, or empty if query returns no rows
df_loaded, latest_date_str = fetch_latest_job_data()

# --- Calculate Metrics ---
# Pass the actual date string to metrics calculation
print("Calculating metrics...")
calculated_metrics = calculate_metrics(df_loaded)
# Ensure the date from connector (even if df is empty) is used if available
if latest_date_str:
     calculated_metrics['latest_ingestion_date'] = latest_date_str
print(f"Metrics calculated. Latest Date: {calculated_metrics['latest_ingestion_date']}")


# --- Generate Figures ---
# generate_dashboard_figures handles None or empty df internally
figures_dict, grouped_keys = generate_dashboard_figures(df_loaded)

# --- Create Layout ---
app.layout = create_layout(calculated_metrics, figures_dict, grouped_keys)

# --- Run the App ---
if __name__ == '__main__':
    # Use host='0.0.0.0' to make it accessible on your network
    app.run(debug=True, host='0.0.0.0', port=8050)