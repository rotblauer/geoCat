


# combine the contents of a list of files into a single pandas dataframe
# returns the combined dataframe
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
