import pandas as pd
import json
import datetime

# deterministic summary for asbestos data

def get_deterministic_summary(data_df: pd.DataFrame) -> dict:
    """
    Calculates deterministic statistics from the asbestos data.

    Args:
        data_df: A pandas DataFrame containing the scoped asbestos data.

    Returns:
        A dictionary (bundle) of summary statistics.
    """

    STATUS_COL = 'STATUS_DESCRIPTION'
    ACM_TYPE_COL = 'ACM_TYPE'
    ACM_AMOUNT_COL = 'ACM_AMOUNT'
    ACM_UNIT_COL = 'ACM_UNIT'
    END_DATE_COL = 'END_DATE'
    FACILITY_TYPE_COL = 'FACILITY_TYPE'     
    ABATEMENT_TYPE_COL = 'ABATEMENT_TYPE'   


    summary_bundle = {}

    # Check if the DataFrame is empty (e.g., no BBLs found)
    if data_df.empty:
        summary_bundle['total_reports'] = 0
        summary_bundle['risk_profile'] = {}
        summary_bundle['facility_type_counts'] = {}
        summary_bundle['abatement_type_counts'] = {}
        summary_bundle['acm_type_counts'] = {}
        summary_bundle['amount_by_unit'] = {}
        summary_bundle['latest_activity_date'] = None
        summary_bundle['recent_activity_count'] = 0
        return summary_bundle

    # 1. Get total count
    summary_bundle['total_reports'] = len(data_df)

    # 2. Create Risk Profile 
    if STATUS_COL in data_df.columns:
        status_counts = data_df[STATUS_COL].value_counts().to_dict()
        summary_bundle['risk_profile'] = {
            'active_permits (HIGH risk)': status_counts.get('Submitted', 0),
            'pending_permits (MED risk)': status_counts.get('Postponed', 0),
            'completed_permits (LOW risk)': status_counts.get('Closed', 0)
        }
    else:
        summary_bundle['risk_profile'] = {"error": f"Column '{STATUS_COL}' not found."}

    # 3. Get counts for Facility Type
    if FACILITY_TYPE_COL in data_df.columns:
        summary_bundle['facility_type_counts'] = data_df[FACILITY_TYPE_COL].value_counts().to_dict()
    else:
        summary_bundle['facility_type_counts'] = {"error": f"Column '{FACILITY_TYPE_COL}' not found."}

    # 4. Get counts for Abatement Type
    if ABATEMENT_TYPE_COL in data_df.columns:
        summary_bundle['abatement_type_counts'] = data_df[ABATEMENT_TYPE_COL].value_counts().to_dict()
    else:
        summary_bundle['abatement_type_counts'] = {"error": f"Column '{ABATEMENT_TYPE_COL}' not found."}

    # 5. Get counts for Asbestos Containing Material (ACM) types
    if ACM_TYPE_COL in data_df.columns:
        summary_bundle['acm_type_counts'] = data_df[ACM_TYPE_COL].value_counts().to_dict()
    else:
        summary_bundle['acm_type_counts'] = {"error": f"Column '{ACM_TYPE_COL}' not found."}

    # 6. Get total amount, grouped by unit (e.g., "Square Feet", "Linear Feet")
    if ACM_AMOUNT_COL in data_df.columns and ACM_UNIT_COL in data_df.columns:
        data_df[ACM_AMOUNT_COL] = pd.to_numeric(data_df[ACM_AMOUNT_COL], errors='coerce')
        summary_bundle['amount_by_unit'] = data_df.groupby(ACM_UNIT_COL)[ACM_AMOUNT_COL].sum().to_dict()
    else:
        summary_bundle['amount_by_unit'] = {"error": "Amount or Unit column not found."}

    # 7. Recency analysis
    if END_DATE_COL in data_df.columns:
        try:
            # Convert to datetime objects ONCE
            end_dates = pd.to_datetime(data_df[END_DATE_COL])
            
            # Get latest date
            summary_bundle['latest_activity_date'] = end_dates.max().strftime('%Y-%m-%d')
            
            # Get recent count (in the last 2 years)
            two_years_ago = pd.Timestamp.now() - pd.DateOffset(years=2)
            recent_count = (end_dates > two_years_ago).sum()
            summary_bundle['recent_activity_count'] = int(recent_count) # Convert to standard int
            
        except Exception as e:
            summary_bundle['latest_activity_date'] = f"Error processing dates: {e}"
            summary_bundle['recent_activity_count'] = 0
    else:
        summary_bundle['latest_activity_date'] = {"error": f"Column '{END_DATE_COL}' not found."}
        summary_bundle['recent_activity_count'] = 0


    return summary_bundle


def load_scoped_data() -> pd.DataFrame:
    """
    Helper function to load the scoped data.
    This simulates what your DataHandler would do.
    """
    try:
        df = pd.read_csv('scoped_asbestos_data.csv')
        return df
    except FileNotFoundError:
        print("Error: 'scoped_asbestos_data.csv' not found.")
        print("Please run the 'asbestos_scoping.py' script first.")
        return pd.DataFrame() # Return empty DataFrame on error



# TESTING

if __name__ == "__main__":
    
    print("--- Testing Deterministic Summary Function ---")
    
    # 1. Load the data (simulating DataHandler)
    scoped_data = load_scoped_data()
    
    if not scoped_data.empty:
        print(f"Loaded {len(scoped_data)} rows from 'scoped_asbestos_data.csv'")
        
        # 2. Generate the summary bundle
        summary = get_deterministic_summary(scoped_data)
        
        # 3. Print the result
        print("\n--- Summary Bundle ---")
        # Use json.dumps for a clean print
        print(json.dumps(summary, indent=2, default=str)) 

