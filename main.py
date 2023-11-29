import concurrent
import json
import os
import sys
from pathlib import Path

import geopandas
from geopandas import GeoDataFrame

import utilities
import countCat
import lineIterator
from concurrent.futures import ProcessPoolExecutor


def get_state_count_output_file(root_output):
    return root_output + '_state_count.csv'


def get_country_count_output_file(root_output):
    return root_output + '_country_count.csv'


def get_state_shp():
    data_dir = Path(os.getcwd()) / 'data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return utilities.prepare_us_county_shapefile(data_dir)


def get_country_count_output_file(root_output):
    return root_output + '_country_count.csv'


def get_country_shp():
    data_dir = Path(os.getcwd()) / 'data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return utilities.prepare_country_shapefile(data_dir)


def get_activity_count_output_file(root_output):
    return root_output + '_activity_count.csv'


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


def get_county_columns_to_group_by():
    return ['STATE_NAME', 'COUNTYFP', 'NAME', 'Name']


def get_country_columns_to_group_by():
    return ['SOVEREIGNT', 'SOV_A3', 'Name']


def summarize(features, root_output, crs):
    print("summarizing root " + root_output)
    # converts the list of string features to a list of json features

    if not os.path.exists(get_state_count_output_file(root_output)):
        features = [json.loads(feature) for feature in features]
        gdf = GeoDataFrame.from_features(features, crs=crs)
        joined_states = utilities.spatial_join(gdf, get_state_shp())
        summarized_states = countCat.count_cat(joined_states, get_county_columns_to_group_by())
        summarized_states.to_csv(get_state_count_output_file(root_output), header=True)

    return "done with " + root_output


# summarizes catTracks from https://catonmap.net for downstream analysis
# the input is a json file of catTracks piped to stdin

if __name__ == '__main__':
    # print column names of the country shapefile
    print(geopandas.read_file(get_country_shp()).columns)

    # print the first 5 rows of the country shapefile
    print(geopandas.read_file(get_country_shp()).head())
    exit(0)
    outD = Path(os.getcwd()) / 'output'
    if not os.path.exists(outD):
        os.makedirs(outD)
    bs = 1000000
    crsSet = "EPSG:4269"
    iter_j = lineIterator.LineIterator(sys.stdin)
    process_tracks(iter_j, bs, outD, crsSet)
