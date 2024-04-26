"""
DataFrame utils not provided by Pandas
"""


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


