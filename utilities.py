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
        # # add a header to the request so the server does not reject it
        opener = urllib.request.build_opener()
        opener.addheaders = [
            ('User-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0')]
        urllib.request.install_opener(opener)
        # download the file using the header
        try:
            urllib.request.urlretrieve(url, shapefile_file)
        except urllib.error.HTTPError as e:
            print(e.code)
            print(e.read())
    return shapefile_file


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


# takes a geopandas dataframe and a list of columns
# adds a new column of day/month/year to the dataframe using the add_date_columns method
# groups by the columns and the date columns and counts the number of each category in the dataset
# returns a pandas dataframe with the counts
def summarize(gdf, columns, timestamp_column):
    # add the date columns to the dataframe
    gdf, date_column = add_date_columns(gdf, timestamp_column)

    # group by the columns and the date columns and count the number of each category
    cat_counts = gdf.groupby(columns + date_column).size().reset_index(name='counts')

    # return the counts
    return cat_counts


# adds a new column of day/month/year to the dataframe
# the date is extracted from the timestamp column
# returns the dataframe with the new columns, and the names of the new columns
# takes a geopandas dataframe and a timestamp column name
def add_date_columns(gdf, timestamp_column):
    # convert the timestamp column to a datetime
    gdf[timestamp_column] = geopandas.to_datetime(gdf[timestamp_column])

    # extract the day, month, and year from the timestamp column
    gdf['date'] = gdf[timestamp_column].dt.date
    # return the dataframe with the new columns
    return gdf, ['date']


# joins the catTracks geopandas dataframe with another geopandas dataframe
# and counts the number of tracks per category using count_cat method
# returns a pandas dataframe with the counts
def summarize_tracks(gdf, join_gdf, columns):
    # perform the spatial join
    joined_gdf = spatial_join(gdf, join_gdf)

    # count the number of tracks per category
    return count_cat(joined_gdf, columns)
