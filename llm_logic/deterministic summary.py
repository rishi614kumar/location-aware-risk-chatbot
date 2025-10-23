

## Deterministic summary for asbestos data
import pandas as pd
import json
import datetime
import os  

def get_deterministic_summary(data_df: pd.DataFrame) -> dict:
    """
    Calculates deterministic statistics from a DataFrame.

    This function uses a WEIGHTED RISK MODEL to generate a score,
    providing a more granular summary than simple counts.

    Args:
        data_df: A pandas DataFrame containing asbestos data.

    Returns:
        A dictionary (bundle) of summary statistics.
    """
    
    # ---
    # Column names from the CSV
    # ---
    STATUS_COL = 'STATUS_DESCRIPTION'
    ACM_TYPE_COL = 'ACM_TYPE'
    ACM_AMOUNT_COL = 'ACM_AMOUNT'
    ACM_UNIT_COL = 'ACM_UNIT'
    END_DATE_COL = 'END_DATE'
    FACILITY_TYPE_COL = 'FACILITY_TYPE'
    ABATEMENT_TYPE_COL = 'ABATEMENT_TYPE'
    # ---
    
    # ---
    #  Weighted Risk Model Definitions
    # ---
    # Friable (crumbly, airborne) materials get high weights
    MATERIAL_RISK_WEIGHTS = {
        # High Risk
        'Pipe Insulation': 3.0,
        'Boiler Insulation': 3.0,
        'Spray-on Fireproofing': 3.0,
        # Medium Risk
        'Roofing': 2.0,
        'Siding': 2.0,
        # Low Risk (non-friable)
        'Floor Tile': 1.0,
        'Window Caulking': 1.0,
        'Mastic': 1.0,
        'default': 0.5 # For any other/unknown material
    }
    
    # Active permits get a high weight
    STATUS_RISK_WEIGHTS = {
        'Submitted': 1.5, # Active risk
        'Postponed': 1.0, # Pending risk
        'Closed': 0.2,    # Historical risk
        'default': 0.1
    }
    # ---

    summary_bundle = {}

    # Check if the DataFrame is empty (e.g., no BBLs found)
    if data_df.empty:
        summary_bundle['total_reports'] = 0
        # NEW: Return the new risk_profile structure
        summary_bundle['risk_profile'] = {
            'total_risk_score': 0,
            'active_permit_count': 0,
            'completed_permit_count': 0,
            'highest_risk_material_found': 'None'
        }
        summary_bundle['facility_type_counts'] = {}
        summary_bundle['abatement_type_counts'] = {}
        summary_bundle['acm_type_counts'] = {}
        summary_bundle['amount_by_unit'] = {}
        summary_bundle['latest_activity_date'] = None
        summary_bundle['recent_activity_count'] = 0
        return summary_bundle

    # 1. Get total count
    summary_bundle['total_reports'] = len(data_df)

    # 2. NEW: Create Advanced Risk Profile
    if STATUS_COL in data_df.columns and ACM_TYPE_COL in data_df.columns:
        
        # --- Create risk columns for each permit ---
        # Map weights, using 'default' if a type isn't in our list
        data_df['material_weight'] = data_df[ACM_TYPE_COL].apply(
            lambda x: MATERIAL_RISK_WEIGHTS.get(x, MATERIAL_RISK_WEIGHTS['default'])
        )
        data_df['status_weight'] = data_df[STATUS_COL].apply(
            lambda x: STATUS_RISK_WEIGHTS.get(x, STATUS_RISK_WEIGHTS['default'])
        )
        
        # Calculate the score for *each permit*
        data_df['permit_risk_score'] = data_df['material_weight'] * data_df['status_weight']
        
        # --- Aggregate scores for the final profile ---
        total_risk_score = data_df['permit_risk_score'].sum()
        
        # Find the name of the highest risk material present
        highest_risk_material = data_df.loc[data_df['material_weight'].idxmax()][ACM_TYPE_COL]
        
        status_counts = data_df[STATUS_COL].value_counts().to_dict()
        
        summary_bundle['risk_profile'] = {
            'total_risk_score': round(total_risk_score, 2),
            'active_permit_count': status_counts.get('Submitted', 0),
            'completed_permit_count': status_counts.get('Closed', 0),
            'highest_risk_material_found': highest_risk_material
        }
    else:
        summary_bundle['risk_profile'] = {"error": f"Missing '{STATUS_COL}' or '{ACM_TYPE_COL}' column."}

    # 3. Get counts for Facility Type (e.g., "Commercial")
    if FACILITY_TYPE_COL in data_df.columns:
        summary_bundle['facility_type_counts'] = data_df[FACILITY_TYPE_COL].value_counts().to_dict()
    else:
        summary_bundle['facility_type_counts'] = {"error": f"Column '{FACILITY_TYPE_COL}' not found."}

    # 4. Get counts for Abatement Type (e.g., "Removal") 
    if ABATEMENT_TYPE_COL in data_df.columns:
        summary_bundle['abatement_type_counts'] = data_df[ABATEMENT_TYPE_COL].value_counts().to_dict()
    else:
        summary_bundle['abatement_type_counts'] = {"error": f"Column '{ABATEMENT_TYPE_COL}' not found."}

    # 5. Get counts for Asbestos Containing Material (ACM) types 
    if ACM_TYPE_COL in data_df.columns:
        summary_bundle['acm_type_counts'] = data_df[ACM_TYPE_COL].value_counts().to_dict()
    else:
        summary_bundle['acm_type_counts'] = {"error": f"Column '{ACM_TYPE_COL}' not found."}

    # 6. Get total amount, grouped by unit - (Still useful)
    if ACM_AMOUNT_COL in data_df.columns and ACM_UNIT_COL in data_df.columns:
        # Ensure amount is numeric before grouping
        data_df[ACM_AMOUNT_COL] = pd.to_numeric(data_df[ACM_AMOUNT_COL], errors='coerce')
        # Group by the unit, sum the amount, and convert to dictionary
        summary_bundle['amount_by_unit'] = data_df.groupby(ACM_UNIT_COL)[ACM_AMOUNT_COL].sum().to_dict()
    else:
        summary_bundle['amount_by_unit'] = {"error": "Amount or Unit column not found."}

    # 7. Recency analysis (latest date and count of recent jobs) 
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
    
    This version is ROBUST and will find the CSV file
    whether you run the script from the root folder or
    from the 'llm_logic' subfolder.
    """
    try:
        # --- ROBUST PATH LOGIC ---
        # Get the directory where this script (risk_summarizer.py) is located
        script_dir = os.path.dirname(os.path.realpath(__file__))
        
        # Go one level up ('..') to the project root and find the CSV
        csv_path = os.path.join(script_dir, '..', 'scoped_asbestos_data.csv')
        
        # Normalize the path (e.g., llm_logic/../scoped_asbestos_data.csv -> scoped_asbestos_data.csv)
        csv_path = os.path.normpath(csv_path)

        df = pd.read_csv(csv_path)
        return df
    except FileNotFoundError:
        print(f"Error: Could not find 'scoped_asbestos_data.csv'.")
        print(f"Looked for it at: {csv_path}")
        print("Please make sure 'scoped_asbestos_data.csv' is in your root project folder.")
        return pd.DataFrame() # Return empty DataFrame on error
    except Exception as e:
        # Catch other potential errors
        print(f"An error occurred in load_scoped_data: {e}")
        return pd.DataFrame()


# ---
#  TESTING 
# ---
if __name__ == "__main__":
    
    print("--- Testing Deterministic Summary Function ---")
    
    # 1. Load ALL scoped data (simulating DataHandler)
    all_scoped_data = load_scoped_data()
    
    if not all_scoped_data.empty:
        print(f"\nLoaded {len(all_scoped_data)} total rows from 'scoped_asbestos_data.csv'")
        
        # ---
        # Summary for the ENTIRE dataset (13 rows)
        # ---
        print("\n--- TEST 1: Summary for ALL 13 rows (for overall testing) ---")
        summary_all = get_deterministic_summary(all_scoped_data.copy())
        print(json.dumps(summary_all, indent=2, default=str))

      
