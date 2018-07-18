from django.shortcuts import render
# from uploader.models import UploadForm,Upload
from django.http import HttpResponseRedirect
from django.urls import reverse
import csv, json
import os
import codecs
import pandas as pd
import requests
import logging
import time

#----------Logging Configuration
logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)
# create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

#----------Configuration
API_KEY="YOUR_API_KEY_HERE"
BACKOFF_TIME=30
col_name="Address"
RETURN_FULL_RESULTS = False
output_filedir = os.path.abspath(os.getcwd())+"/media/files"
output_filepath = output_filedir+"/geoCoded.csv"
# Create your views here.
def home(request):
    if request.POST and request.FILES:
        csvfile = request.FILES['csv_file']
        #-----------------Read file
        data = pd.read_csv(csvfile, encoding='utf8')
        if col_name not in data.columns:
            print("Please input a file which has a column \"Address\"")

        #-----------------Generate address list
        address_list = data[col_name].tolist()

        #----------------Loop to process address_list
        results = []
        for address in address_list:
            geocodedFlag = False     #flag to ensure an address is run only once through the while loop

            while geocodedFlag is not True:
                # Initiate the geocoding procedure
                try:
                    geocodeResult = hitGoogleAPI(address, API_KEY, return_full_response=RETURN_FULL_RESULTS)
                except Exception as e:
                    logger.exception(e)
                    logger.error("Skipping this address. Exception found {}".format(address))
                    geocodedFlag = True

                if geocodeResult['Status'] == 'OVER_QUERY_LIMIT': #------------------Check if API limit reached
                    logger.info("Query limit hit! Pausing the geocoding process.")
                    time.sleep(BACKOFF_TIME * 60) #----------------------------------Program paused for 30 minutes
                    geocodedFlag = True
                else: #--------------------------------------------------------------API limit not reached, continue processing
                    if geocodeResult['Status']!='OK':
                        logger.warning("Error returned in geocoding {}: {}".format(address, geocodeResult['Status']))
                    else:
                        logger.debug("Address geocoded successfully: {}: {}".format(address, geocodeResult['Status']))
                    results.append(geocodeResult)
                    geocodedFlag=True

        logger.info("Geocoding finished for this file")


        #------------------Checking if the output directory is present
        if (os.path.isdir(output_filedir)):
            logging.info("Output directory present. Presenting file for download.")
            pd.DataFrame(results).to_csv(output_filepath, encoding='utf-8')
            return render(request, 'home.html',{'download':"/media/files/geoCoded.csv", 'file_name':"geoCodedAddresses.csv", 'label':"Here's the Geo Coded file: "})
        else:#-------------Directory not present. Create new and then write the file to location
            try:
                os.makedirs(output_filedir)
            except OSError:
                logging.info("Directory creation failed. Exiting program")
                return render(request, 'home.html',{'error_msg':"Output directory creation failed"})
            else:
                logging.info("Output directory created or is already present. Presenting file for download")
                pd.DataFrame(results).to_csv(output_filepath, encoding='utf-8')
                return render(request, 'home.html',{'download':"/media/files/geoCoded.csv",'file_name':"geoCodedAddresses.csv", 'label':"Here's the Geo Coded file: "})

    return render(request, 'home.html',{'error_msg':"GeoCoding failed!!"})


def hitGoogleAPI(address, api_key, return_full_response=False):
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}".format(address,api_key)

    #Ping the google API
    results = requests.get(geocode_url)
    #Convert the results
    results = results.json()

    #Check if the result list is empty
    if len(results['results']) == 0:
        output = {
            "Actual_Address" : None,
            "Latitude": None,
            "Longitude": None
        }
    else:
        answer = results['results'][0]
        output = {
            "Actual_Address" : answer.get('formatted_address'),
            "Latitude": answer.get('geometry').get('location').get('lat'),
            "Longitude": answer.get('geometry').get('location').get('lng')
        }

    output['Address'] = address
    output['Number_Hits_On_Address'] = len(results['results'])
    output['Status'] = results.get('status')
    return output
