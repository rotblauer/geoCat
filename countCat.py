# summarize the number of each category in the dataset

# groups by a list of columns and counts the number of each category in the dataset
# returns a pandas dataframe with the counts
def count_cat(gdf, columns):
    # if the columns do not exist in the dataframe, set them to empty strings
    for column in columns:
        if column not in gdf.columns:
            gdf[column] = ''
    # group by the columns and count the number of each category
    cat_counts = gdf.groupby(columns).size().reset_index(name='counts')

    # return the counts
    return cat_counts
