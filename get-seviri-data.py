#!/usr/bin/python
import sys, os, requests, re, urllib3, json, zipfile, urllib.parse, urllib.request
from datetime import datetime
from datetime import timedelta

# Get authentication
r = requests.post(
    'https://api.eumetsat.int/token',
    data="grant_type=client_credentials",
    headers={'Authorization': 'Basic {}'.format(os.environ['DATAPORTAL_AUTH'])}
)
res = json.loads(r.text)
access_token = res['access_token']


# API base endpoint
apis_endpoint= "http://api.eumetsat.int"
# Searching endpoint
service_search = apis_endpoint + "/data/search-products/os"
# Downloading endpoint
service_download = apis_endpoint + "/data/download"
# Collection id from data portal
collection_id='EO:EUM:DAT:MSG:MSG15-RSS'
# Download destination
dest = '/tmp/data'
if not os.path.exists(dest):
    os.makedirs(dest)

# Define our polygon for spatial subsetting
coordinates = [[-1.0, -1.0],[-1.0, 70.0],[84.0, 70.0],[-1.0, 70.0],[-1, -1.0]]

# Define our start and end dates for temporal subsetting
end_date = datetime.now()
start_date = end_date - timedelta(days = 1)

# METADATA SEARCH

# Format our paramters for searching
dataset_parameters = {'format': 'json', 'pi': collection_id}
dataset_parameters['dtstart'] = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
dataset_parameters['dtend'] = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
dataset_parameters['geo'] = 'POLYGON(({}))'.format(','.join(["{} {}".format(*coord) for coord in coordinates]))

# Retrieve datasets that match our filter
url = service_search
response = requests.get(url, dataset_parameters)
found_data_sets = response.json()

print('Found Datasets: '+str(found_data_sets['properties']['totalResults']))

dataset_parameters['si'] = 0
items_per_page = 10

all_found_data_sets = []
while dataset_parameters['si'] < found_data_sets['properties']['totalResults']:
    response = requests.get(service_search, dataset_parameters)
    found_data_sets = response.json()
    all_found_data_sets.append(found_data_sets)
    dataset_parameters['si'] = dataset_parameters['si'] + items_per_page

# DOWNLOAD DATA

if all_found_data_sets:
    for found_data_sets in all_found_data_sets:
        for selected_data_set in found_data_sets['features']:
            product_id = selected_data_set['properties']['identifier']
            download_url = service_download + '/collections/{}/products/{}'\
              .format(urllib.parse.quote(collection_id),urllib.parse.quote(product_id))+ '?access_token=' + access_token

            filename = dest+'/'+collection_id.replace(':','_') + '_'+product_id
            print('Downloading from {} into {}...'.format(download_url, filename))                    
            urllib.request.urlretrieve(download_url, filename)

            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall(dest)
else:
    print('No data sets found')



