# combine the contents of a list of files into a single pandas dataframe
# returns the combined dataframe
import os

import pandas


# write a pandas dataframe to a csv file
def write_dataframe_to_csv(dataframe, file):
    dataframe.to_csv(file, index=False)


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


# combines the files in adirectory into a single pandas dataframe and writes it to a csv file
# returns the combined dataframe
def combine_and_write(directory, suffix, output_file):
    # load the file paths
    file_paths = load_files(directory, suffix)

    # combine the files into a single dataframe
    combined_dataframe = combine_files(file_paths)

    # sums duplicates in all columns except the counts column
    summed_duplicates = sum_duplicates(combined_dataframe, list(combined_dataframe.columns[:-1]), 'counts')

    # write the dataframe to a csv file in the output directory

    write_dataframe_to_csv(summed_duplicates, os.path.join(directory, output_file))

    # return the combined dataframe
    return combined_dataframe


# find rows that contain the same values in a set of columns
# returns a pandas dataframe with the rows that contain the same values in the columns
def find_duplicates(dataframe, columns):
    # find the duplicates
    duplicates = dataframe[dataframe.duplicated(columns, keep=False)]

    # return the duplicates
    return duplicates


# sum a column in a pandas dataframe when rows have the same values in a set of columns
# returns a pandas dataframe with the summed column
def sum_duplicates(dataframe, columns, column_to_sum):
    # find the duplicates
    duplicates = find_duplicates(dataframe, columns)
    # group by the columns and sum the column to sum
    summed_duplicates = duplicates.groupby(columns)[column_to_sum].sum().reset_index()

    # return the summed duplicates
    return summed_duplicates


# examle of the aggregate function
def example_aggregate():
    # create a pandas datafr
    dataframe = pandas.DataFrame({'a': [1, 2, 1, 2], 'b': [1, 2, 1, 2], 'c': [1, 2, 1, 2], 'counts': [1, 2, 50, 100]})
    # aggregate the dataframe by the a column
    print(dataframe)
    aggregated_dataframe = sum_duplicates(dataframe, ['a', 'b'], ['counts'])
    # print the aggregated dataframe
    print(aggregated_dataframe)


# __main__ is the entry point of the program
if __name__ == '__main__':
    example_aggregate()
