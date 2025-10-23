import pandas as pd

# street BBLs 
bbls_to_keep = [
    4000520001, 4000520005, 4000520007, 4000520017, 4000520020, 
    4000527501, 4000527502, 4000540004, 4000540006, 4000540138, 
    4000540139, 4000540140, 4000540141, 4000540142, 4000540143, 
    4000540144, 4000540145, 4000540146, 4000540147, 4000540148, 
    4000540149, 4000540150, 4000547501, 4000770001, 4000770004, 
    4000770055, 4000770057, 4000777501
]

asbestos_df = pd.read_csv('Asbestos_Control_Program_(ACP7)_20251022.csv')


BOROUGH_COL_NAME = 'BOROUGH' 
BLOCK_COL_NAME = 'BLOCK'     
LOT_COL_NAME = 'LOT'         


# Build the 10-Digit BBL 
asbestos_df = asbestos_df.dropna(subset=[BOROUGH_COL_NAME, BLOCK_COL_NAME, LOT_COL_NAME])

asbestos_df['block_int'] = asbestos_df[BLOCK_COL_NAME].astype(int)
asbestos_df['lot_int'] = asbestos_df[LOT_COL_NAME].astype(int)

borough_map = {
    'Manhattan': 1,
    'Bronx': 2,
    'Brooklyn': 3,
    'Queens': 4,
    'Staten Island': 5
}
asbestos_df['borough_code'] = asbestos_df[BOROUGH_COL_NAME].str.strip().map(borough_map)


asbestos_df = asbestos_df.dropna(subset=['borough_code'])

asbestos_df['BBL_str'] = (
    asbestos_df['borough_code'].astype(int).astype(str) +  
    asbestos_df['block_int'].astype(str).str.zfill(5) +    
    asbestos_df['lot_int'].astype(str).str.zfill(4)      
)

# Convert the new BBL string to a number
asbestos_df['BBL_10_digit'] = pd.to_numeric(asbestos_df['BBL_str'], errors='coerce')


#  Filter the DataFrame 
scoped_df = asbestos_df[asbestos_df['BBL_10_digit'].isin(bbls_to_keep)]

# Save the scoped data
scoped_df.to_csv('scoped_asbestos_data.csv', index=False)


print(f"Filtering complete! Found {len(scoped_df)} matching rows.")


    