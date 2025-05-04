# dashboard/utils/processing.py
import pandas as pd

def calculate_metrics(df):
    """Calculates summary metrics from the job data DataFrame."""
    metrics = {'latest_ingestion_date': 'N/A'} # Will be overwritten if df has date
    if df is None or df.empty:
        print("Calculating metrics on empty DataFrame.")
        return {
            'total_jobs': 0, 'unique_companies': 0, 'avg_min_exp': 'N/A',
            'common_role': 'N/A', 'common_location': 'N/A', 'remote_percentage': 'N/A',
            **metrics
        }

    # --- Safely calculate metrics ---
    total_jobs = len(df)
    unique_companies = df['company_name'].nunique() if 'company_name' in df.columns else 0

    avg_min_exp = 'N/A'
    # Check if 'required_qualifications' exists, is not all null, and first non-null is a dict
    if ('required_qualifications' in df.columns and
            not df['required_qualifications'].isnull().all() and
            isinstance(df['required_qualifications'].dropna().iloc[0], dict)):
        try:
            exp_series = df['required_qualifications'].apply(
                lambda x: x.get('experience_years_min') if isinstance(x, dict) else None
            ).dropna()
            if not exp_series.empty:
                 # Attempt conversion safely
                 numeric_exp = pd.to_numeric(exp_series, errors='coerce').dropna()
                 if not numeric_exp.empty:
                     avg_min_exp = round(numeric_exp.mean(), 1)
        except Exception as e:
            print(f"Warning: Error calculating avg_min_exp: {e}")


    common_role = 'N/A'
    # Check if 'job_summary' exists, is not all null, and first non-null is a dict
    if ('job_summary' in df.columns and
            not df['job_summary'].isnull().all() and
            isinstance(df['job_summary'].dropna().iloc[0], dict)):
        try:
            role_series = df['job_summary'].apply(
                lambda x: x.get('role_title') if isinstance(x, dict) else None
            ).dropna().astype(str) # Convert potential non-strings
            if not role_series.empty:
                common_role = role_series.mode()[0] if not role_series.mode().empty else 'N/A'
        except Exception as e:
             print(f"Warning: Error calculating common_role: {e}")


    common_location = df['location'].mode()[0] if 'location' in df.columns and not df['location'].mode().empty else 'N/A'

    remote_percentage = 'N/A'
    if 'workplace_type' in df.columns and total_jobs > 0:
        try:
            remote_count = df['workplace_type'].astype(str).str.contains('Remote', case=False, na=False).sum()
            remote_percentage = round((remote_count / total_jobs) * 100, 1)
        except Exception as e:
             print(f"Warning: Error calculating remote_percentage: {e}")


    # Get latest date directly from the DataFrame if available
    if 'ingestionDate' in df.columns and not df['ingestionDate'].isnull().all():
         try:
             latest_date_obj = pd.to_datetime(df['ingestionDate'].dropna().iloc[0])
             metrics['latest_ingestion_date'] = latest_date_obj.strftime('%Y-%m-%d')
         except Exception as e:
             print(f"Warning: Error formatting ingestionDate: {e}")


    return {
        'total_jobs': total_jobs, 'unique_companies': unique_companies, 'avg_min_exp': avg_min_exp,
        'common_role': common_role, 'common_location': common_location, 'remote_percentage': remote_percentage,
        **metrics
    }