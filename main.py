import argparse
import concurrent
import json
import os

import geopandas

import consolidate
import lineIterator
import sys
from geopandas import GeoDataFrame

import utilities
from concurrent.futures import ProcessPoolExecutor

suffixes = ['_state_count.csv', '_country_count.csv', '_activity_count.csv']


def get_state_count_output_file(root_output):
    return root_output + suffixes[0]


def get_country_output(root_output):
    return root_output + suffixes[1]


def get_activity_count_output_file(root_output):
    return root_output + suffixes[2]


def get_county_columns_to_group_by():
    return ['STATE_NAME','STATEFP', 'COUNTYFP', 'NAME', 'Name']


def get_country_columns_to_group_by():
    return ['SOVEREIGNT', 'SOV_A3', 'Name']


def get_activity_columns_to_group_by():
    return ['Activity', 'Name']


def completion_callback(future):
    print(future.result())


def load_features(file, crs):
    return geopandas.read_file(file, crs=crs)


# tests if the state and country and activity results already exist
# returns true if they do, false otherwise
def results_complete(root_output):
    return os.path.exists(get_state_count_output_file(root_output)) and os.path.exists(
        get_country_output(root_output)) and os.path.exists(get_activity_count_output_file(root_output))


def summarize(features, root_output, crs, skip_existing):
    print("summarizing root " + root_output)
    # converts the list of string features to a list of json features

    if not results_complete(root_output):
        features = [json.loads(feature) for feature in features]
        gdf = GeoDataFrame.from_features(features, crs=crs)

        # if the state summary file does not exist, create it
        state_output_file=get_state_count_output_file(root_output)
        if skip_existing and os.path.exists(state_output_file):
            print("skipping state summary", state_output_file)
        else:

            print("summarizing states", state_output_file)
            # summarize the tracks by state
            summarized_states = utilities.summarize_tracks(gdf, load_features(utilities.get_state_shp(), crs=crs),
                                                           get_county_columns_to_group_by())
            summarized_states.to_csv(state_output_file, header=True,index=False)

        # if the country summary file does not exist, create it
        country_output_file=get_country_output(root_output)
        if skip_existing and os.path.exists(country_output_file):
            print("skipping countries", country_output_file)
        else:
            # summarize the tracks by country

            print("summarizing countries", country_output_file)
            summarized_countries = utilities.summarize_tracks(gdf,
                                                              load_features(utilities.get_country_shp(),
                                                                            crs=crs).to_crs(crs),
                                                              get_country_columns_to_group_by())
            summarized_countries.to_csv(country_output_file, header=True, index=False)

        # if the activity summary file does not exist, create it
        activity_output_file=get_activity_count_output_file(root_output)
        if skip_existing and os.path.exists(activity_output_file):
            print("skipping activities", activity_output_file)
        else:
            print("summarizing activities", activity_output_file)
            # summarize the tracks by activity
            summarized_activities = utilities.summarize_by_date(gdf, get_activity_columns_to_group_by(),
                                                                timestamp_column='Time')
            # do not write the index to the file
            summarized_activities.to_csv(activity_output_file, header=True, index=False)

    return "done with " + root_output


def process_tracks(iterator, batch_size, output_dir, crs, workers, skip_existing):
    current_batch = 0
    batch_iterator = utilities.batcher(iterator, batch_size)

    # If workers is 0, do not use concurrent futures executor
    if workers == 0:
        for features in batch_iterator:
            root_output = os.path.join(output_dir,
                                       "batch." + str(current_batch) + ".size." + str(
                                           len(features)))
            completion_callback(summarize(features, root_output, crs))
            current_batch += 1
        return

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for features in batch_iterator:
            root_output = os.path.join(output_dir,
                                       "batch." + str(current_batch) + ".size." + str(
                                           len(features)))
            future = executor.submit(summarize, features, root_output, crs, skip_existing)
            future.add_done_callback(completion_callback)
            current_batch += 1


def get_combined_output_file(directory, suffix):
    return directory + suffix + "_combined.csv"


# concatenates the results files in a directory into a single file
# uses the suffix to determine which files to concatenate
# returns the combined dataframe
def consolidate_results_files(output_dir):
    # get the output file
    for suffix in suffixes:
        output_file = get_combined_output_file(output_dir, suffix)
        print("consolidating " + output_file)
        # if the output file does not exist, create it
        if not os.path.exists(output_file):
            # load the files
            consolidate.combine_and_write(output_dir, suffix, output_file)


if __name__ == '__main__':

    # parse the command line arguments
    parser = argparse.ArgumentParser(description='Summarizes the tracks in a json file')
    parser.add_argument('--batch_size', type=int, default=500000,
                        help='the number of features to process at a time')
    parser.add_argument('--output_dir', type=str, default='output',
                        help='the directory to write the output files to')
    # argument for crs
    parser.add_argument('--crs', type=str, default='EPSG:4269',
                        help='the crs to use for the data')
    # argument for workers
    parser.add_argument('--workers', type=int, default=8,
                        help='the number of workers to use')
    # argument to skip processing
    parser.add_argument('--skip_existing', action='store_true', help='skip processing if output files exist')
    parser.add_argument('--skip_process_tracks', action='store_true', help='skip tracks processing step')

    args = parser.parse_args()

    outD = args.output_dir
    print("output dir is " + outD)
    if not os.path.exists(outD):
        os.makedirs(outD)
    if not args.skip_process_tracks:
        print("processing tracks")
        process_tracks(lineIterator.LineIterator(sys.stdin), args.batch_size, args.output_dir, args.crs,
                       args.workers, args.skip_existing)
    print("consolidating results")
    consolidate_results_files(args.output_dir)
