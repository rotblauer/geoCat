import os
import urllib
from pathlib import Path

import geopandas


# batches lines of features into a list of size batch_size
def batcher(iterator, batch_size):
    # create an empty list to hold the features
    features = []
    # iterate through the features
    for feature in iterator:
        # add the feature to the list
        features.append(feature)

        # if the length of the list is equal to the batch size
        if len(features) == batch_size:
            # return the list of features
            yield features
            # reset the list of features
            features = []
    # if the batch size is 0 or the last batch is less than the batch size
    if len(features) > 0:
        yield features


# performs a spatial join between two geopandas dataframes
# returns a geopandas dataframe with the attributes from both dataframes
# and the geometry from the left dataframe
def spatial_join(left_gdf, right_gdf):
    # perform the spatial join
    spatial_join_gdf = geopandas.sjoin(left_gdf, right_gdf, how='inner', predicate='intersects')

    # return the spatial join geopandas dataframe
    return spatial_join_gdf


# downloads the us county shapefile from the census ftp site
# and saves it to the data directory
# if the file already exists, it does not download it again
# returns the path to the shapefile
def prepare_us_county_shapefile(data_dir):
    # set the census directory
    census_dir = os.path.join(data_dir, 'census')

    # create the census directory if it does not exist
    Path(census_dir).mkdir(parents=True, exist_ok=True)

    # set the shapefile file name
    shapefile_file = os.path.join(census_dir, 'cb_2020_us_county_500k.zip')

    # if the shapefile file does not exist, download it
    if not os.path.isfile(shapefile_file):
        # set the url
        url = 'https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip'

        # download the file
        urllib.request.urlretrieve(url, shapefile_file)

    # return the path to the shapefile
    return shapefile_file


# downloads the country shapefile from the naturalearthdata site
# and saves it to the data directory
# if the file already exists, it does not download it again
# returns the path to the shapefile
def prepare_country_shapefile(data_dir):
    # set the naturalearthdata directory
    naturalearthdata_dir = os.path.join(data_dir, 'naturalearthdata')

    # create the naturalearthdata directory if it does not exist
    Path(naturalearthdata_dir).mkdir(parents=True, exist_ok=True)

    # set the shapefile file name
    shapefile_file = os.path.join(naturalearthdata_dir, 'ne_50m_admin_0_countries.zip')

    # if the shapefile file does not exist, download it
    if not os.path.isfile(shapefile_file):
        # set the url
        url = 'https://www.naturalearthdata.com/http//www.naturalearthdata.com/download/50m/cultural/ne_50m_admin_0_countries.zip'

        # download the file
        urllib.request.urlretrieve(url, shapefile_file)

    # return the path to the shapefile
    return shapefile_file
