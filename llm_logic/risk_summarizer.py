import pandas as pd
import json
from typing import List, Dict, Any
import random


# 1. NTA ADAPTER 
from adapters.nta import get_bbls_from_nta 

# 2. asbestos_data  
def get_asbestos_data(bbl_list: List[str]) -> pd.DataFrame:
    """
    Simulates retrieving raw asbestos violation data for a list of BBLs.
    This simulation now uses a FIXED dataset for BBLs in the RADIUS_BBLS_TEST_LIST 
    to ensure repeatable testing of the aggregation logic.
    """
    
    # --- FIXED MOCK DATA SET ---
    # Total of 41 BBLs analyzed by the teammate
    NUM_BBLS = len(bbl_list)
    
    # Create fixed data to ensure the risk tiering logic is testable:
    # 1. 5 BBLs have 5 violations and 100 units (High Risk)
    # 2. 10 BBLs have 1 violation and 20 units (Medium Risk)
    # 3. The rest (26 BBLs) have 0 violations and 50 units (Low Risk)
    
    violations_data = [5] * 5 + [1] * 10 + [0] * (NUM_BBLS - 15)
    unit_data = [100] * 5 + [20] * 10 + [50] * (NUM_BBLS - 15)
    
    
    # We will use the BBL list directly only if the size matches the expected 41 for the test case
    if NUM_BBLS == 41:
        data = {
            'BBL': bbl_list,
            'ASBESTOS_VIOLATIONS': violations_data,
            'UNIT_COUNT': unit_data,
        }
    else:
        # Fallback to general simulation if the input list size is different (e.g., full NTA)
        data = {
            'BBL': bbl_list,
            'ASBESTOS_VIOLATIONS': [random.choice([0, 0, 0, 1, 2]) for _ in bbl_list],
            'UNIT_COUNT': [random.randint(10, 100) for _ in bbl_list],
        }

    return pd.DataFrame(data)

# --- CORE AGGREGATION & CHECKING LOGIC ---

def aggregate_risks(raw_data_df: pd.DataFrame, nta_name: str) -> Dict[str, Any]:
    """
    Stage 1: Condenses raw BBL risk data into a compact, structured summary dictionary.
    This performs the core data dimensionality reduction.
    """
    if raw_data_df.empty:
        # Handle the case where the NTA is found, but no matching risk data exists
        return {"Error": f"No detailed Asbestos data available for properties in {nta_name}."}

    # 1. Calculate Base Metrics
    total_bbls = len(raw_data_df)
    total_violations = raw_data_df['ASBESTOS_VIOLATIONS'].sum()
    total_units = raw_data_df['UNIT_COUNT'].sum()

    # 2. Calculate Key Risk Metrics
    
    # Identify BBLs with ANY violation (> 0)
    risky_bbls_df = raw_data_df[raw_data_df['ASBESTOS_VIOLATIONS'] > 0]
    bbls_with_risk_count = len(risky_bbls_df)
    
    # Calculate Incidence
    risk_incidence_percent = round((bbls_with_risk_count / total_bbls) * 100, 1)

    # 3. Apply Categorization/Tiers (CHECKING)
    # The LLM needs a simple tier, not just a percentage.
    if bbls_with_risk_count == 0:
        risk_tier = 'NONE'
    elif risk_incidence_percent > 30:
        risk_tier = 'HIGH (Widespread Concern)'
    elif risk_incidence_percent > 10:
        risk_tier = 'MEDIUM (Elevated Incidence)'
    else:
        risk_tier = 'LOW (Minimal Incidence)'

    # 4. Return the highly condensed, structured summary
    return {
        "SummaryType": "Asbestos Risk Profile",
        "LocationName": nta_name,
        "TotalPropertiesAnalyzed": total_bbls,
        "ASBESTOS_PROFILE": {
            "RISK_TIER": risk_tier,
            "INCIDENCE_PERCENT": f"{risk_incidence_percent}%",
            "TOTAL_VIOLATIONS_COUNT": int(total_violations),
            "PROPERTIES_AT_RISK": bbls_with_risk_count,
            "VIOLATIONS_PER_100_UNITS": round((total_violations / total_units) * 100, 2) if total_units > 0 else 0
        }
    }


def format_for_llm_payload(aggregated_data: Dict[str, Any]) -> str:
    """
    Stage 2: Converts the Python dictionary into a concise, templated JSON string.
    This is the final input ready for the LLM API call.
    """
    # The use of json.dumps with indent=2 ensures the LLM receives clean, structured data.
    return json.dumps(aggregated_data, indent=2)


# --- MAIN PIPELINE FUNCTION ---

def generate_nta_risk_summary(nta_code: str, nta_name: str) -> str:
    """
    Executes the full pipeline: NTA -> BBLs -> Raw Data -> Structured Summary -> LLM Input.
    """
    print(f"Starting Asbestos Risk Analysis for {nta_name} (NTA: {nta_code})...")
    
    # 1. RETRIEVE PROPERTIES 
    # This calls the real adapter function imported from the 'nta' module.
    bbl_list = get_bbls_from_nta(nta_code) 

    if not bbl_list:
        return format_for_llm_payload({"Error": f"NTA found, but no BBLs returned for {nta_name}."})

    print(f"   -> Found {len(bbl_list)} properties. Retrieving raw data...")
    
    # 2. RETRIEVE RAW RISK DATA (Uses teammate's data loader)
    # The dependency is here. This function must be provided by your teammate.
    raw_risk_data = get_asbestos_data(bbl_list)
    
    # 3. AGGREGATE SCORES & CHECKING
    print("   -> Aggregating data and applying risk tiering...")
    structured_summary = aggregate_risks(raw_risk_data, nta_name)

    # 4. FORMAT AND RETURN LLM PAYLOAD (Templated Summary Logic)
    return format_for_llm_payload(structured_summary)

# --- EXAMPLE EXECUTION ---
# Case 1: Harlem NTA (Full neighborhood analysis)
NTA_CODE_HARLEM = 'MN11'
NTA_NAME_HARLEM = 'Harlem'

# Case 2: Queens NTA 
NTA_CODE_QUEENS = 'QN31'
NTA_NAME_QUEENS = 'Hunters Point-Sunnyside-West Maspeth'

# Example Test Case 3: Radius Mode (using teammate's provided BBL list)
RADIUS_BBLS_TEST_LIST = [
    '4000547501', '4000527501', '4000527502', '4000540150', '4000540149', 
    '4000540148', '4000540147', '4000520007', '4000540146', '4000540145', 
    '4000520005', '4000540144', '4000520017', '4000540143', '4000540006', 
    '4000540142', '4000540141', '4000540140', '4000520001', '4000540138', 
    '4000540139', '4000540137', '4000540004', '4000520020', '4000770004', 
    '4000770057', '4000777501', '4000770055', '4000770001', '4000500127', 
    '4000500124', '4000500025', '4000500026', '4000500024', '4000500023', 
    '4000500027', '4000500022', '4000500021', '4000780001', '4000500028', 
    '4000760047'
]

def generate_radius_risk_summary(bbl_list: List[str], location_context: str) -> str:
    """
    A helper function to test the aggregation logic directly with a list of BBLs
    (simulating a radius mode query).
    """
    print(f"Starting Radius Analysis for {location_context} ({len(bbl_list)} properties)...")
    
    # 1. RETRIEVE RAW RISK DATA 
    raw_risk_data = get_asbestos_data(bbl_list)
    
    # 2. AGGREGATE SCORES & CHECKING
    print("   -> Aggregating data and applying risk tiering...")
    structured_summary = aggregate_risks(raw_risk_data, location_context)
    
    # 3. FORMAT AND RETURN LLM PAYLOAD
    return format_for_llm_payload(structured_summary)

# testing: Radius Mode aggregation logic
radius_payload = generate_radius_risk_summary(RADIUS_BBLS_TEST_LIST, "100 ft Radius of QN31 Property")
print("\n=======================================================")
print("RADIUS MODE PAYLOAD (Testing Aggregation Logic)")
print("=======================================================")
print(radius_payload)

# testing full QN31 NTA mode analysis
nta_payload_queens = generate_nta_risk_summary(NTA_CODE_QUEENS, NTA_NAME_QUEENS)
print("\n=======================================================")
print(f"FULL NTA PAYLOAD (NTA: {NTA_CODE_QUEENS})")
print("=======================================================")
print(nta_payload_queens)