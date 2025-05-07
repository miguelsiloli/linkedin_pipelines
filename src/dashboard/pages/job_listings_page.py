# dashboard/pages/job_listings_page.py

import dash
from dash import dcc, html, callback, Input, Output, State
import pandas as pd
import numpy as np

# --- Helper Functions ---

def format_field(label, value):
    """Formats a label and value pair, handling None."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None # Don't display if value is None or empty string
    return html.P([html.Strong(f"{label}: "), str(value)])

def format_list_field(label, value_list):
    """
    Formats a label and a list/array of values, handling None, empty list,
    or empty numpy array.
    """
    is_empty = False
    if value_list is None:
        is_empty = True
    elif isinstance(value_list, np.ndarray):
        is_empty = value_list.size == 0 # Check NumPy array size
    elif isinstance(value_list, list):
        is_empty = not value_list # Check standard list truthiness
    # Add checks for other potential iterables if necessary

    if is_empty:
        return None # Don't display if None, empty list, or empty array

    # Filter out None or empty strings/values from the list/array before displaying
    # Also handle potential non-string items safely with str()
    items = [str(item) for item in value_list if item is not None and str(item).strip()]

    if not items: # Check if filtering resulted in an empty list
        return None

    return html.Div([
        html.Strong(f"{label}:"),
        html.Ul([html.Li(item) for item in items], style={'marginTop': '2px', 'marginBottom': '5px'})
    ])

def format_bool_field(label, value):
    """Formats a boolean field nicely."""
    if value is None:
        return None
    display_value = "Yes" if value else "No"
    return html.P([html.Strong(f"{label}: "), display_value])

def build_structured_info(job_data):
    """Builds the structured information div from job data Series."""
    if job_data is None or job_data.empty:
        return html.P("No data found for this job.")

    # Safely access data using .get() for dictionaries/structs
    job_summary = job_data.get('job_summary', {}) or {}
    company_info = job_data.get('company_information', {}) or {}
    location_model = job_data.get('location_and_work_model', {}) or {}
    req_qual = job_data.get('required_qualifications', {}) or {}
    tech_skills = req_qual.get('technical_skills', {}) or {} # Nested under required_qualifications
    pref_qual = job_data.get('preferred_qualifications', {}) or {}
    role_context = job_data.get('role_context', {}) or {}
    benefits = job_data.get('benefits', {}) or {}

    info_elements = [
        html.H4("Job Overview", style={'marginBottom':'10px', 'borderBottom':'1px solid #eee', 'paddingBottom':'5px'}),
        format_field("Company", job_data.get('company_name')),
        format_field("Role", job_summary.get('role_title')),
        format_field("Seniority", job_summary.get('role_seniority')),
        format_field("Employment Type", job_data.get('employment_type')),
        format_field("Experience Level", job_data.get('experience_level')),
        format_field("Workplace", job_data.get('workplace_type')),
        format_list_field("Locations", location_model.get('locations')),
        format_bool_field("Visa Sponsorship", job_summary.get('visa_sponsorship')),
        html.Hr(style={'margin': '15px 0'}),

        html.H4("Company Details", style={'marginBottom':'10px', 'borderBottom':'1px solid #eee', 'paddingBottom':'5px'}),
        format_field("Company Type", company_info.get('company_type')),
        format_list_field("Company Values", company_info.get('company_values_keywords')),
        html.Hr(style={'margin': '15px 0'}),

        # --- Simplified Qualifications Display ---
        # You could expand this to show all skill arrays if desired
        html.H4("Qualifications Summary", style={'marginBottom':'10px', 'borderBottom':'1px solid #eee', 'paddingBottom':'5px'}),
        format_field("Experience Min (Yrs)", req_qual.get('experience_years_min')),
        format_field("Experience Max (Yrs)", req_qual.get('experience_years_max')),
        format_field("Education", req_qual.get('education_requirements')),
        format_list_field("Key Tech Platforms", tech_skills.get('cloud_platforms')),
        format_list_field("Key Languages", tech_skills.get('programming_languages', {}).get('general_purpose')),
        format_list_field("Key Databases", tech_skills.get('database_technologies')),
        format_list_field("Methodologies", req_qual.get('methodologies_practices')),
        format_list_field("Soft Skills", req_qual.get('soft_skills_keywords')),
        format_list_field("Preferred Skills", pref_qual.get('skills_keywords')),

        # Add more sections as needed (Role Context, Benefits etc.) following the same pattern
    ]

    # Filter out None elements before returning
    return html.Div([element for element in info_elements if element is not None])


# --- Main Layout Function ---
def create_layout(df):
    """Creates the layout for the job listings page."""
    print("Creating job listings layout...") # Add print statement for debugging

    if df is None or df.empty:
        return html.Div([
            html.H3("Job Listings"),
            html.P("No job data available to display.")
        ])

    # Create the list of jobs for the left pane
    # Select only necessary columns for the list view for efficiency
    list_view_df = df[['job_id', 'job_title', 'company_name', 'location']].copy()
    list_view_df.fillna({'job_title':'N/A', 'company_name':'N/A', 'location':'N/A'}, inplace=True) # Handle potential NaNs

    job_list_items = []
    for index, row in list_view_df.iterrows():
        job_id = row['job_id']
        job_list_items.append(
            html.Div([
                dcc.Link(
                    html.H5(f"{row['job_title']}", style={'marginBottom': '2px'}),
                    href=f'/jobs/{job_id}', # Link URL includes job_id
                    className='job-list-link'
                 ),
                 html.P(f"{row['company_name']} - {row['location']}", style={'fontSize': '0.9em', 'color': 'grey', 'marginTop':'0px'})
            ], className='job-list-item', id=f'job-list-item-{job_id}') # Add ID for potential styling/interaction later
        )

    # --- Assemble page layout ---
    layout = html.Div([
        html.Div(job_list_items, className='job-list-pane', id='job-list-scroll'), # Left, scrollable pane
        html.Div(id='job-detail-pane', className='job-detail-pane'), # Right pane, content updated by callback
    ], className='job-browser-container') # Main container for flexbox/grid

    return layout

# --- Callback to Update Job Details ---
# This callback uses the main 'app' instance defined in app.py
# and accesses the globally loaded 'df_loaded' from app.py
# It needs df_loaded to be accessible in the scope where callbacks are processed.

# Import the app instance and df_loaded from the main app file
# This creates a potential circular import if not handled carefully.
# A better way for larger apps is using dcc.Store, but this works for now.
from ..app import app, df_loaded # Use relative import

@callback(
    Output('job-detail-pane', 'children'),
    Input('url', 'pathname')
)
def update_job_details(pathname):
    print(f"Job detail callback triggered. Pathname: {pathname}")

    # Only check if the pathname itself is valid for displaying job details
    if not pathname or not pathname.startswith('/jobs/'):
        # This handles the load of '/jobs' itself, or '/' or other invalid states
        print("Job detail callback: Pathname not specific to a job ID.")
        return html.P("Select a job from the list to see details.", style={'padding': '20px', 'textAlign': 'center'})

    # --- Now, proceed only if pathname looks like /jobs/job_id ---

    # Check if df_loaded exists and is not empty (can stay here or move up)
    if df_loaded is None or df_loaded.empty:
            print("Job detail callback: df_loaded is None or empty.")
            return html.P("Job data is not available.", style={'padding': '20px'})

    # --- The rest of the try...except block to extract job_id and build layout ---
    try:
        # Extract job_id from the pathname
        path_parts = pathname.split('/')
        # Make sure there's something *after* /jobs/
        if len(path_parts) < 3 or not path_parts[2]:
                print("Job detail callback: Could not extract job_id from path.")
                return html.P("Invalid job link format.", style={'padding': '20px'})
        selected_job_id = path_parts[2]
        print(f"Job detail callback: Searching for job_id: {selected_job_id}")

        # ... (rest of the existing try block: find job_data_series, check if empty, get job_data, build layout) ...
        job_data_series = df_loaded[df_loaded['job_id'] == selected_job_id]

        if job_data_series.empty:
            print(f"Job detail callback: Job ID {selected_job_id} not found.")
            return html.P(f"Job ID {selected_job_id} not found.", style={'padding': '20px'})

        job_data = job_data_series.iloc[0]
        print(f"Job detail callback: Found data for {selected_job_id}")

        job_description = job_data.get('job_description', 'No description available.')
        structured_info_div = build_structured_info(job_data)
        job_link = job_data.get('job_link')


        detail_layout = html.Div([
                html.H3(job_data.get('job_title', 'Job Details'), style={'marginBottom':'5px'}),
                html.P(
                    # Start with the base elements
                    [
                        html.Strong(job_data.get('company_name', 'N/A')),
                        " - ",
                        html.Em(job_data.get('location', 'N/A')),
                    ]
                    # Conditionally extend the list if job_link exists
                    + ([" | ", dcc.Link("Original Posting", href=job_link, target="_blank", style={'marginLeft':'10px'})] if job_link else []),
                    style={'marginTop':'0px', 'marginBottom':'15px', 'color':'#555'}
                ),
                html.Hr(),
                html.Div([
                html.Div([
                    html.H4("Job Description"),
                    dcc.Markdown(job_description, link_target="_blank", className='job-description-markdown')
                ], className='job-description-container'),
                html.Div(structured_info_div, className='structured-info-container')
                ], className='job-detail-inner-container')
        ], style={'padding': '15px'})

        return detail_layout

    except Exception as e:
        print(f"Error in update_job_details callback: {e}")
        import traceback
        traceback.print_exc()
        return html.P("An error occurred while loading job details.", style={'padding': '20px', 'color': 'red'})