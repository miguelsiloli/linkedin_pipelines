# dashboard/components/figures.py

import plotly.express as px
import pandas as pd
from collections import Counter, OrderedDict
import ast
import numpy as np


# --- Plot Configuration ---
# Paths for the "General" Tab
general_paths = [
    ['company_name'],
    ['job_summary', 'role_title'],
    ['company_information', 'company_type'],
    ['job_summary', 'role_seniority'],
    ['company_information', 'company_values_keywords'],
    ['location_and_work_model', 'locations'],
]

# Paths for Skill/Concept Tabs
skill_paths = [
    ['required_qualifications', 'technical_skills', 'cloud_platforms'],
    ['required_qualifications', 'technical_skills', 'database_technologies'],
    ['required_qualifications', 'technical_skills', 'programming_languages', 'general_purpose'],
    ['required_qualifications', 'technical_skills', 'programming_languages', 'scripting_frontend'],
    ['required_qualifications', 'technical_skills', 'programming_languages', 'query'],
    ['required_qualifications', 'technical_skills', 'programming_languages', 'data_ml_libs'],
    ['required_qualifications', 'technical_skills', 'programming_languages', 'platform_runtime'],
    ['required_qualifications', 'technical_skills', 'programming_languages', 'configuration'],
    ['required_qualifications', 'technical_skills', 'programming_languages', 'other_specialized'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'data_modeling'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'data_storage_paradigms'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'etl_elt_pipelines'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'data_governance_quality'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'architecture_patterns'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'big_data_concepts'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'cloud_data_architecture'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'ml_ai_data_concepts'],
    ['required_qualifications', 'technical_skills', 'data_architecture_concepts', 'core_principles_optimization'],
    ['required_qualifications', 'technical_skills', 'data_visualization_bi_tools'],
    ['required_qualifications', 'technical_skills', 'cloud_services_tools'],
    ['required_qualifications', 'technical_skills', 'etl_integration_tools'],
    ['required_qualifications', 'technical_skills', 'devops_mlops_ci_cd_tools'],
    ['required_qualifications', 'technical_skills', 'orchestration_workflow_tools'],
    ['required_qualifications', 'technical_skills', 'other_tools'],
    ['required_qualifications','methodologies_practices'],
    ['required_qualifications','soft_skills_keywords'],
]

# Default Tab Names
default_tech_tab = "Core Technical Skills"
default_qual_tab = "Other Qualifications"

# --- Visualization Function ---
def create_plotly_bar_chart(df, column_path, n=10, title_prefix="Top"):
    """
    Generates a Plotly bar chart for top N frequent items.
    Handles columns containing lists/arrays (flattens them) AND
    columns containing simple values (like strings - uses value_counts).
    """
    # (Keep the exact working version of this function from the previous step)
    print(f"\n--- Generating chart for: {' -> '.join(column_path)} ---")
    chart_title_suffix = column_path[-1].replace('_', ' ').title() # For error messages

    # --- 1. Access the target column data safely ---
    current_data = df
    valid_path = True
    try:
        for key in column_path:
            if isinstance(current_data, pd.DataFrame) and key in current_data.columns:
                current_data = current_data[key]
            elif isinstance(current_data, pd.Series):
                current_data = current_data.apply(lambda x: x.get(key) if isinstance(x, dict) else None)
            else:
                print(f"Error: Could not access key '{key}'. Current data type: {type(current_data)}")
                valid_path = False; break

            if isinstance(current_data, pd.Series):
                current_data = current_data.dropna()
                if current_data.empty:
                    print(f"Info: No data remaining after accessing key '{key}' and dropping Nones.")
                    valid_path = False; break
    except Exception as e:
        print(f"Exception during data access for path {' -> '.join(column_path)}: {e}")
        valid_path = False

    # Handle errors or empty data after access
    if not valid_path or current_data.empty:
        print(f"No valid data found or accessible at path {' -> '.join(column_path)}.")
        fig_title = f"{title_prefix} {n} {chart_title_suffix} - Path Error or No Data"
        return px.bar(title=fig_title).update_layout(
            xaxis={'visible': False}, yaxis={'visible': False},
            annotations=[dict(text="Data not found", showarrow=False)]
        )

    if not isinstance(current_data, pd.Series):
         print(f"Error: Accessed data is not a Pandas Series for {' -> '.join(column_path)}. Type: {type(current_data)}")
         fig_title = f"{title_prefix} {n} {chart_title_suffix} - Processing Error"
         return px.bar(title=fig_title).update_layout(
            xaxis={'visible': False}, yaxis={'visible': False},
            annotations=[dict(text="Internal Error", showarrow=False)]
        )

    # --- 2. Count Frequencies (Logic depends on data type) ---
    item_counts = None
    # Check the type of the *first non-null* element to decide strategy
    first_valid_element = current_data.iloc[0] if not current_data.empty else None

    if isinstance(first_valid_element, (list, np.ndarray)):
        print("Detected list/array type column. Flattening...")
        all_items = []
        for item_container in current_data: # Iterate through the Series
            items_to_add = []
            if isinstance(item_container, (list, np.ndarray)):
                 items_to_add = [item for item in item_container if item is not None and isinstance(item, (str, int, float, bool))]
            all_items.extend(items_to_add)
        if not all_items: print("No hashable items found after flattening list/array column.")
        else: item_counts = Counter(all_items)
    else:
        print("Detected simple value type column. Using value_counts().")
        try:
             item_counts = current_data.astype(str).value_counts()
        except Exception as e:
             print(f"Error using value_counts on column {column_path[-1]}: {e}")
             item_counts = None

    # --- 3. Handle No Counts Found ---
    is_empty = False
    if item_counts is None: is_empty = True
    elif isinstance(item_counts, pd.Series): is_empty = item_counts.empty
    elif isinstance(item_counts, Counter): is_empty = not item_counts

    if is_empty:
        print(f"No frequency counts generated for {' -> '.join(column_path)}.")
        fig_title = f"{title_prefix} {n} {chart_title_suffix} - No Counts"
        return px.bar(title=fig_title).update_layout(
            xaxis={'visible': False}, yaxis={'visible': False},
            annotations=[dict(text="No counts available", showarrow=False)]
        )

    # --- 4. Get Top N ---
    if isinstance(item_counts, pd.Series):
        top_items_series = item_counts.head(n)
        plot_df = top_items_series.reset_index(); plot_df.columns = ['Item', 'Frequency']
    elif isinstance(item_counts, Counter):
        top_items_list = item_counts.most_common(n)
        if not top_items_list:
             print(f"No top items found for {' -> '.join(column_path)}.")
             fig_title = f"{title_prefix} {n} {chart_title_suffix} - No Top Items"
             return px.bar(title=fig_title).update_layout(xaxis={'visible': False}, yaxis={'visible': False}, annotations=[dict(text="No top items found", showarrow=False)])
        plot_df = pd.DataFrame(top_items_list, columns=['Item', 'Frequency'])
    else:
        print("Error: Unexpected type for item_counts.")
        return px.bar(title=f"{title_prefix} {n} {chart_title_suffix} - Internal Error")

    # --- 5. Create Plotly Plot ---
    fig_title = f"{title_prefix} {n} {chart_title_suffix}"
    fig = px.bar(plot_df, y='Item', x='Frequency', orientation='h', title=fig_title, labels={'Item':'', 'Frequency':'Frequency Count'})
    fig.update_layout(
        yaxis={'categoryorder':'total ascending', 'type': 'category'}, title_x=0.5,
        margin=dict(l=10, r=10, t=40, b=10), height=max(300, len(plot_df) * 30 + 60)
    )
    print(f"Successfully generated plot for: {' -> '.join(column_path)}")
    return fig


# --- Helper to Create Placeholder Figures ---
def create_placeholder_fig(path):
     fig_key = path[-1]
     fig_title = f"Top 10 {fig_key.replace('_', ' ').title()} - No Data Available"
     return fig_key, px.bar(title=fig_title).update_layout(
         annotations=[dict(text="Data not loaded", showarrow=False)]
     )


# --- Main Function to Generate All Figures and Groupings ---
def generate_dashboard_figures(df):
    """
    Generates all figures for the dashboard and groups them by tab.

    Args:
        df (pd.DataFrame): The input DataFrame. Can be None or empty.

    Returns:
        tuple: (dict, OrderedDict):
               - figures: Dictionary mapping figure keys to Plotly figure objects.
               - grouped_figure_keys: OrderedDict mapping tab labels to lists of figure keys.
    """
    figures_dict = {}
    grouped_keys = OrderedDict()
    grouped_keys["General"] = [] # Initialize General tab first

    print("\n--- Generating and Grouping Plotly Figures ---")

    data_available = df is not None and not df.empty

    # Process General Paths
    print("--- Processing General Paths ---")
    for path in general_paths:
        fig_key = path[-1]
        if data_available:
            fig_object = create_plotly_bar_chart(df, path, n=15, title_prefix="Top")
        else:
            _, fig_object = create_placeholder_fig(path) # Use placeholder helper

        figures_dict[fig_key] = fig_object
        grouped_keys["General"].append(fig_key)

    # Process Skill Paths
    print("--- Processing Skill Paths ---")
    for path in skill_paths:
        fig_key = path[-1]
        if data_available:
            fig_object = create_plotly_bar_chart(df, path, n=10, title_prefix="Top")
        else:
             _, fig_object = create_placeholder_fig(path) # Use placeholder helper

        figures_dict[fig_key] = fig_object

        # Determine Tab Label
        tab_label = None
        if len(path) >= 4 and path[1:3] == ['technical_skills', 'programming_languages']:
            tab_label = "Programming Languages"
        elif len(path) >= 4 and path[1:3] == ['technical_skills', 'data_architecture_concepts']:
             tab_label = "Data Architecture Concepts"
        elif len(path) == 3 and path[1] == 'technical_skills':
            tab_label = default_tech_tab
        elif len(path) >= 2 and path[0] == 'required_qualifications':
            tab_label = default_qual_tab
        else:
            tab_label = "Miscellaneous" # Fallback

        if tab_label not in grouped_keys:
            grouped_keys[tab_label] = []
        grouped_keys[tab_label].append(fig_key)

    if not data_available:
        print("DataFrame was empty or None. Placeholder figures generated.")

    return figures_dict, grouped_keys