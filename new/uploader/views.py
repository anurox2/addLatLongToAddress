from django.shortcuts import render
# from uploader.models import UploadForm,Upload
from django.http import HttpResponseRedirect
from django.urls import reverse
import csv, json
import codecs
import pandas as pd
import requests
import logging
import time

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

        #----------------loop to process address_list
        results = []
        for address in address_list:
            geocodedFlag = False     #flag to ensure an address is run only once through the while loop

            while geocodedFlag is not True:
                # Initiate the geocoding procedure
                try:
                    geocodeResult = hitGoogleAPI(address, API_KEY, return_full_response=RETURN_FULL_RESULTS)
                except Exception as e:
                    logger.exception(e)
                    logger.error("Skipping this address. Error returned {}".format(address))
                    geocodedFlag = True

                if geocodeResult['status'] == 'OVER_QUERY_LIMIT': #-------------Check if API limit reached
                    logger.info("Query limit hit! Pausing the geocoding process.")
                    time.sleep(BACKOFF_TIME * 60) #--------program paused for 30 minutes
                    geocodedFlag = True
                else: #-----------------API limit not reached, continue processing
                    if geocodeResult['status']!='OK':
                        logger.warning("Error returned in geocoding {}: {}".format(address, geocodeResult['status']))
                    else:
                        logger.debug("Address geocoded successfully: {}: {}".format(address, geocodeResult['status']))
                    results.append(geocodeResult)
                    geocodedFlag=True

        logger.info("Geocoding finished for this file")

        #------------Write data back to file
        # pd.DataFrame(results).to_csv(data, encoding='utf-8')

        #------------------
        header ="formatted_address,input_string,latitude,longitude,number_of_hits_on_address,status"
        with open("geoCoded.csv", "w", newline="") as var:
            header = header.split(",")
            write = csv.DictWriter(var, header, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            write.writeheader()
            write.writerows(results)



    return render(request,'home.html')
    # return render(request,'home.html',{'form':file,'files':files})

#formatted_address,input_string,latitude,longitude, number_of_hits_on_address, status


def hitGoogleAPI(address, api_key, return_full_response=False):
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}&key={}".format(address,api_key)

    #Ping the google API
    results = requests.get(geocode_url)
    #Convert the results
    results = results.json()

    #Check if the result list is empty
    if len(results['results']) == 0:
        output = {
            "formatted_address" : None,
            "latitude": None,
            "longitude": None
        }
    else:
        answer = results['results'][0]
        output = {
            "formatted_address" : answer.get('formatted_address'),
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng')
        }

    output['input_string'] = address
    output['number_of_hits_on_address'] = len(results['results'])
    output['status'] = results.get('status')
    return output
