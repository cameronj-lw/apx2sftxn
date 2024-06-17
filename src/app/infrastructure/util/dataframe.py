"""
DataFrame utils not provided by Pandas
"""

import pandas as pd

from typing import Any, Dict, List


def delete_rows(df, dict_to_delete, in_place=True):
    # Iterate over dictionary items and construct boolean masks
    mask = None
    for column, value in dict_to_delete.items():
        column_mask = df[column] == value
        if mask is None:
            mask = column_mask
        else:
            mask &= column_mask

    if in_place:
        # Modify DataFrame in-place
        df.drop(df[mask].index, inplace=True)
    else:
        # Return a new DataFrame
        new_df = df[~mask]
        return new_df

# def df_to_dict(df, pk_col_name) -> Dict[Any, Dict[str, Any]]:
#     # Convert DataFrame to dictionary with orient='index'
#     dict_of_dicts = df.to_dict(orient='index')

#     # Initialize an empty dictionary to store the final result
#     result_dict = {}

#     # Iterate over the dictionary items
#     for idx, row_dict in dict_of_dicts.items():
#         # Extract the PK value from the row dictionary
#         pk_value = row_dict[pk_col_name]
        
#         # Remove the PK column from the row dictionary
#         del row_dict[pk_col_name]
        
#         # Add the row dictionary to the result dictionary with PK value as the key
#         result_dict[pk_value] = row_dict

#     return result_dict

def df_to_dict(df, pk_col_names: List[str]) -> Dict[Any, Dict[str, Any]]:
    # Initialize an empty dictionary to store the final result
    result_dict = {}

    # Iterate over the rows of the DataFrame
    for _, row in df.iterrows():
        # Create a tuple of values from the PK columns
        pk_values = tuple(row[col] for col in pk_col_names)
        # Add the values (excluding PK columns) to the dictionary using the composite key
        result_dict[pk_values] = {key: row[key] for key in df.columns if key not in pk_col_names}

    return result_dict


# Function to compare two dataframes row by row
def compare_dataframes(df1, df2, match_columns, exclude_columns, tolerances={}, ignore_zeros_vs_none=True):
    # Initialize lists to store matched rows, unmatched rows, and matching rows with differences
    matched_rows = []
    unmatched_rows_df1 = []
    unmatched_rows_df2 = []
    matching_rows_with_differences = []
    
    # Define function to determine if two values are equal
    def are_equal(val1, val2, tolerance=None, ignore_zeros_vs_none=True):
        # Handle np.nan
        if pd.isna(val1) and pd.isna(val2):
            return True

        # Surface zero vs None as diffs (if specified)
        if not ignore_zeros_vs_none:
            if tolerance:
                return (abs(val1 - val2 <= tolerance))
            else:
                return val1 == val2
        
        # From here below only applies for ignore_zeros_vs_none
        if (val1 == 0 or pd.isna(val1) or val1 is None) and (val2 == 0 or pd.isna(val2) or val2 is None):
            return True
        # if pd.isna(val1) and pd.isna(val2):
        #     return True
        # if val1 is None and val2 is None:
        #     return True
        # if val1 == 0 and val2 == 0:
        #     return True

        if tolerance:
            return (abs(val1 - val2 <= tolerance))
        else:
            return val1 == val2
    
    # Iterate over rows in df1
    for index, row1 in df1.iterrows():
        # Filter corresponding row in df2 based on match_columns
        filter_condition = (df2[match_columns] == row1[match_columns]).all(axis=1)
        filtered_df2 = df2.loc[filter_condition]
        
        if len(filtered_df2) == 0:
            unmatched_rows_df1.append(row1)
        else:
            # Iterate over filtered rows in df2
            for index2, row2 in filtered_df2.iterrows():
                # Check if the values of selected columns match
                if all(are_equal(row1[col], row2[col], tolerances.get(col), ignore_zeros_vs_none) for col in df1.columns if col not in exclude_columns):
                    matched_rows.append((row1, row2))
                else:
                    # Find non-matching columns
                    non_matching_columns = [col for col in df1.columns if col not in exclude_columns and not are_equal(row1[col], row2[col], tolerances.get(col), ignore_zeros_vs_none)]
                    matching_rows_with_differences.append((row1, row2, non_matching_columns))
    
    # Find unmatched rows in df2
    for index, row2 in df2.iterrows():
        # Filter corresponding row in df1 based on match_columns
        filter_condition = (df1[match_columns] == row2[match_columns]).all(axis=1)
        filtered_df1 = df1.loc[filter_condition]
        if len(filtered_df1) == 0:
            unmatched_rows_df2.append(row2)
    
    # Print results
    print(f"Matched rows with differences: {len(matching_rows_with_differences)}")
    for row1, row2, non_matching_columns in matching_rows_with_differences:
        # print("Table 1:")
        # print(row1)
        # print("Table 2:")
        # print(row2)
        print(f"{row1['tran_code']} {row1['local_tran_key']} Non-matching columns: {len(non_matching_columns)}")
        for col in non_matching_columns:
            print(f"{col}: {row1[col]} (Table 1) vs {row2[col]} (Table 2)")
        print("\n")
    
    print(f"Unmatched rows in Table 1: {len(unmatched_rows_df1)}")
    for row1 in unmatched_rows_df1:
        print(f"{row1['tran_code']} {row1['local_tran_key']}")
    
    print(f"Unmatched rows in Table 2: {len(unmatched_rows_df2)}")
    for row2 in unmatched_rows_df2:
        print(f"{row2['tran_code']} {row2['local_tran_key']}")
    
    print(f"Matched rows: {len(matched_rows)}")
    # for row1, row2 in matched_rows:
    #     print("Table 1:")
    #     print(row1)
    #     print("Table 2:")
    #     print(row2)
    #     print("\n")

