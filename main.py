import concurrent
import json
import os
from pathlib import Path

import geopandas

import lineIterator
import sys
from geopandas import GeoDataFrame

import utilities
from concurrent.futures import ProcessPoolExecutor


def get_state_count_output_file(root_output):
    return root_output + '_state_count.csv'


def get_country_output(root_output):
    return root_output + '_country_count.csv'


def get_activity_count_output_file(root_output):
    return root_output + '_activity_count.csv'


def get_county_columns_to_group_by():
    return ['STATE_NAME', 'COUNTYFP', 'NAME', 'Name']


def get_country_columns_to_group_by():
    return ['SOVEREIGNT', 'SOV_A3', 'Name']


def get_activity_columns_to_group_by():
    return ['Activity', 'Name']


def get_state_shp():
    data_dir = Path(os.getcwd()) / 'data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return utilities.prepare_us_county_shapefile(data_dir)


def get_country_shp():
    data_dir = Path(os.getcwd()) / 'data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return utilities.prepare_country_shapefile(data_dir)


def process_tracks(iterator, batch_size, output_dir, crs):
    current_batch = 0
    batch_iterator = utilities.batcher(iterator, batch_size)

    workers = 8
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for features in batch_iterator:
            root_output = os.path.join(output_dir,
                                       "batch." + str(current_batch) + ".size." + str(
                                           len(features)))
            future = executor.submit(summarize, features, root_output, crs)
            future.add_done_callback(completion_callback)
            current_batch += 1


def completion_callback(future):
    print(future.result())


def load_features(file, crs):
    return geopandas.read_file(file, crs=crs)


# tests if the state and country results already exist
# returns true if they do, false otherwise
def results_exist(root_output):
    return os.path.exists(get_state_count_output_file(root_output)) and os.path.exists(
        get_country_output(root_output))


def summarize(features, root_output, crs):
    print("summarizing root " + root_output)
    # converts the list of string features to a list of json features

    if not results_exist(root_output):
        features = [json.loads(feature) for feature in features]
        gdf = GeoDataFrame.from_features(features, crs=crs)
        # if the state summary file does not exist, create it
        if not os.path.exists(get_state_count_output_file(root_output)):
            print("summarizing states")
            # summarize the tracks by state
            summarized_states = utilities.summarize_tracks(gdf, load_features(get_state_shp(), crs=crs),
                                                           get_county_columns_to_group_by())
            summarized_states.to_csv(get_state_count_output_file(root_output), header=True)
        # if the country summary file does not exist, create it
        if not os.path.exists(get_country_output(root_output)):
            # summarize the tracks by country

            print("summarizing countries")
            summarized_countries = utilities.summarize_tracks(gdf,
                                                              load_features(get_country_shp(), crs=crs).to_crs(crs),
                                                              get_country_columns_to_group_by())
            summarized_countries.to_csv(get_country_output(root_output), header=True)

    return "done with " + root_output

if __name__ == '__main__':

    outD = Path(os.getcwd()) / 'output'
    if not os.path.exists(outD):
        os.makedirs(outD)
    bs = 1000000
    crsSet = "EPSG:4269"
    iter_j = lineIterator.LineIterator(sys.stdin)
    process_tracks(iter_j, bs, outD, crsSet)
