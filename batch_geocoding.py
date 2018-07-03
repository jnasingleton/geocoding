#Python script for batch geocoding of addresses using the Google Geocoding API.
#This script geocodes addresses (including address components) from a specified csv file. 

#Credit (template/framework): https://gist.github.com/shanealynn/033c8a3cacdba8ce03cbe116225ced31

#https://developers.google.com/maps/documentation/geocoding/intro
#https://developers.google.com/maps/documentation/geocoding/start#ComponentFiltering

import numpy as np
import os.path
import pandas as pd
import requests
import logging
import time

# Create logger
logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)

# Create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Add console handler
logger.addHandler(ch)

#------------------ CONFIGURATION -------------------------------

# Set your Google API key here. 
# https://console.developers.google.com/apis
API_KEY = 'GOOGLE_GEOCODING_API_KEY'

# Backoff time sets how many seconds to wait between google pings when your API limit is hit
TIME_BACKOFF = 5

# Set your input file here
FILENAME_INPUT = 'AddressesForGeocoding.csv'

# Set your output file name here.
FILENAME_OUTPUT = FILENAME_INPUT.replace('.csv','_output.csv')

# Return Full Google Results? 
# If True, full JSON results from Google are included in output
RETURN_FULL_RESULTS = False

# Specify the segments/splitcounts for printing and saving
SPLITCOUNT_PRINT = 50
SPLITCOUNT_SAVE = 50

# Fields from import file/dataframe to be used
ADDRESS_IDENTIFIER_FIELD = 'address_id'
ADDRESS_FIELD = 'address'
COMPONENTS_FIELD = 'components'
COMPONENTS2_FIELD = 'components2'
POSTAL_CODE_FIELD = 'postal_code'
LOCALITY_FIELD = 'city'
COUNTRY_FIELD = 'country'

#------------------ DATA LOADING --------------------------------

# Read the data to a Pandas Dataframe
df = pd.read_csv(FILENAME_INPUT, encoding='utf8')

geocoded_results_count_total = df.shape[0]

# Create components field
df[COMPONENTS_FIELD] = df.apply( \
    lambda row: 'postal_code:' + str(row[POSTAL_CODE_FIELD]) + '|' + 'locality:' + str(row[LOCALITY_FIELD]) + '|' + 'country:' + str(row[COUNTRY_FIELD]), axis=1)

# Create components field
df[COMPONENTS2_FIELD] = df.apply( \
    lambda row: 'locality:' + str(row[LOCALITY_FIELD]) + '|' + 'country:' + str(row[COUNTRY_FIELD]), axis=1)

# Check no blanks in required columns

# Create a dataframe to hold results
df_results = None

# Load existing results, if they exist, and adjust df_results
# N.B. This assume a direct row index between these two files
if os.path.isfile(FILENAME_OUTPUT):
    df_results = pd.read_csv(FILENAME_OUTPUT)
    geocoded_results_count = df_results.shape[0]
    df = df.iloc[geocoded_results_count:]
else:
    geocoded_results_count = 0

#------------------	FUNCTION DEFINITIONS ------------------------

def get_google_results(address_identifier, address, components=None, components2=None, api_key=None, return_full_response=False):
    """
    Get first geocode result from Google Maps Geocoding API.
    
    @param address: String address as accurate as possible. 
                        If including the components parameter, exclude those details from the address.
                        Address should not include landmarks, company names, etc.
    @param api_key: String API key from Google. 
    @param return_full_response: Boolean to indicate if you'd like to return the full response from Google. 
                                    This useful if you'd like additional location details for storage or parsing later.

    Future Work: Return a dataframe instead of a dict.
    """

    # Set up simple geocode_url
    geocode_url = 'https://maps.googleapis.com/maps/api/geocode/json?'
    geocode_url += 'address=' + str(address)

    # Set up detailed geocode_url
    if components is not None:
        geocode_url += "&components={}".format(components)
    if api_key is not None:
        geocode_url += "&key={}".format(api_key)
        
    # Retrive the results from Google
    #print(geocode_url)
    results = requests.get(geocode_url)
    # Results will be in JSON format; json() function will decode the JSON format
    results = results.json()

    # If there are no results or an error, 
    # Try again by dropping the postal_code component
    if len(results['results']) == 0:
        geocode_url = geocode_url.replace("&components={}".format(components),"&components={}".format(components2))

        # Retrive the results from Google
        results = requests.get(geocode_url)
        # Results will be in JSON format; json() function will decode the JSON format
        results = results.json()

    # If address is blank or there are no results or an error, return empty results.
    if str(address) == 'nan' or len(results['results']) == 0:
        output = {
            "formatted_address" : None,
            "latitude": None,
            "longitude": None,
            "accuracy": None,
            "google_place_id": None,
            "type": None,
            "postcode": None
        }
        # Append additional details
        output['number_of_results'] = 0
        output['status'] = ''
        output['response'] = ''
    else:    
        # Retrieve only the first result
        answer = results['results'][0]
        output = {
            "formatted_address" : answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng'),
            "accuracy": answer.get('geometry').get('location_type'),
            "google_place_id": answer.get("place_id"),
            "type": ",".join(answer.get('types')),
            "postcode": ",".join([x['long_name'] for x in answer.get('address_components') 
                                  if 'postal_code' in x.get('types')])
        }    
        # Append additional details
        output['number_of_results'] = len(results['results'])
        output['status'] = results.get('status')
        if return_full_response:
            output['response'] = results
    
    # Append the identifier so we can tie back to the import file
    output['identifier'] = address_identifier

    return output

#------------------ PROCESSING LOOP -----------------------------

# Go through each address
for idx_row, row in df.iterrows():

    #idx_row_actual = geocoded_results_count + idx_row
    idx_row_actual = idx_row

    address_identifier = row[ADDRESS_IDENTIFIER_FIELD]
    address = row[ADDRESS_FIELD]
    components = row[COMPONENTS_FIELD]
    components2 = row[COMPONENTS2_FIELD]

    # Continue while the row has not been geocoded:    
    geocoded = False
    while geocoded is not True:
        # Geocode the address with google
        try:
            # Generate compoennts string
            geocode_result = get_google_results(address_identifier, address, components, components2, API_KEY, return_full_response=RETURN_FULL_RESULTS)
        except Exception as e:
            logger.exception(e)
            logger.error("Major error with {}".format(address))
            logger.error("Skipping!")
            geocoded = True
        
        if geocode_result['status'] == 'OVER_QUERY_LIMIT':
            # If API result status is 'OVER_QUERY_LIMIT',
            #  backoff for [] seconds and then retry
            # N.B. This can occur due to the daily limit, limit on queries per second, ...
            logger.info('OVER_QUERY_LIMIT - Backing off for ' + str(TIME_BACKOFF) + ' seconds')
            time.sleep(TIME_BACKOFF)
            geocoded = False
        else:
            # N.B. Results might be empty / not 'OK', these statuses will be logged
            if geocode_result['status'] != 'OK':
                logger.warning("Error geocoding {}: {}".format(address, geocode_result['status']))
            #logger.debug("Geocoded: {}: {}".format(address, geocode_result['status']))
            if df_results is None:
                df_results = pd.DataFrame(geocode_result,index=[0])
            else:
                df_results = df_results.append(geocode_result, ignore_index=True)      
            geocoded = True

    # Print status every [SPLITCOUNT_PRINT] addresses
    if (idx_row_actual+1) % SPLITCOUNT_PRINT == 0:
    	logger.info("Completed {} of {} addresses".format((idx_row_actual+1), geocoded_results_count_total))
            
    # Save df_results to file every [SPLITCOUNT_SAVE] addresses
    if (idx_row_actual+1) % SPLITCOUNT_SAVE == 0:
        df_results.to_csv(FILENAME_OUTPUT, encoding='utf8', index=False)

# Remove duplicate rows, if they exist
df_results.drop_duplicates(inplace=True)

# Save results to file 
df_results.to_csv(FILENAME_OUTPUT, encoding='utf8', index=False)

# Completion message
logger.info("Finished geocoding all addresses")