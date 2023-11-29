# combine the contents of a list of files into a single pandas dataframe
# returns the combined dataframe
import os

import pandas


def combine_files(file_paths):
    # create an empty list to hold the dataframes
    dataframes = []

    # iterate through the file paths
    for file_path in file_paths:
        # read the file as a pandas dataframe
        dataframe = pandas.read_csv(file_path)

        # add the dataframe to the list
        dataframes.append(dataframe)

    # concatenate the dataframes in the list
    combined_dataframe = pandas.concat(dataframes)

    # return the combined dataframe
    return combined_dataframe


# loads files with a given suffix from a directory
# returns a list of file paths
def load_files(directory, suffix):
    # create an empty list to hold the file paths
    file_paths = []

    # iterate through the files in the directory
    for file in os.listdir(directory):
        # if the file ends with the suffix
        if file.endswith(suffix):
            # add the file path to the list
            file_paths.append(os.path.join(directory, file))

    # return the list of file paths
    return file_paths


# joins the geopandas dataframe with the pandas dataframe using an id in common
# returns the joined dataframe
def join_dataframes(gdf, df, id):
    # join the dataframes
    joined_dataframe = gdf.merge(df, on=id)

    # return the joined dataframe
    return joined_dataframe
