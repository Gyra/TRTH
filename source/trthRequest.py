# -*- coding: UTF-8 -*-
"""
This is used to request data from DSS TRTH
Adapted from:
https://github.com/TR-API-Samples/Article.TRTH.Python.REST.OndemandRequestTimeAndSales/blob/master/TickHistoryTimesAndSalesRequest.py
current version: Handing Sun 21/9/2017
"""

import os
import json
import requests
from time import sleep
import pandas as pd

_outputPath = './output/'
_outputName = 'trthTest'
_retryTime = 30.0


def request_new_token(id='', password=''):
    _authenURL = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken'
    _header = {}
    _header['Prefer'] = 'respond-async'
    _header['Content-Type'] = 'application/json; odata.metadata=minimal' # CHECK MEANING
    _data = {'Credentials': {
        'Password': password,
        'Username': id
        }
    }

    # send login request
    resp = requests.post(_authenURL, json=_data, _header=_header)

    # error
    if resp.status_code != 200:
        raise Exception('Authentication Error Status Code: ' + str(resp.status_code) + '. Message: ' + json.dumps(json.loads(resp.text), indent=4))

    return json.loads(resp.text)['value']

def extract_raw(token, json_payload):
    try:
        _extractURL = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'
        _header = {}
        _header['Prefer'] = 'respond-async'
        _header['Content-Type'] = 'application/json; odata.metadata=minimal' # CHECK MEANING
        _header['Accept-Charset'] = 'UTF-8'
        _header['Authorization'] = 'Token ' + token

        # post http request to DSS using extract raw URL
        resp = requests.post(_extractURL, data=None, json=json_payload, headers=_header)
        print('Status Code = ' + str(resp.status_code))

        # raise exception with error message if the return status is not 202 (Accepted) or 200 (Ok)
        if resp.status_code != 200:
            if resp.status_code != 202:
                raise Exception('Error: Status Code = ' + str(resp.status_code) + ' Message: ' + resp.text)

            # get location from header, URL must be https so we need to change it using string replace function
            _location = str.replace(resp.headers['Location'], 'http://', 'https://')
            print('Get status from' + str(_location))

            # pooling loop to check request status every few seconds
            while True:
                resp = requests.get(_location, headers=_header)
                if int(resp.status_code) == 200:
                    break
                else:
                    print('Status: ' + str(resp.headers['Status']))
                sleep(_retryTime)

        # get the job id from http response
        json_resp = json.loads(resp.text)
        _jobID = json_resp.get('JobID')
        print('Status is completed and the JobID is ' + str(_jobID) + '\n')

        # check if the response contains Notes. If the note exists print it to console
        if len(json_resp.get('Notes')) > 0:
            print('Note:\n================================')
            for var in json_resp.get('Notes'):
                print(var)
            print('================================\n')

        # get the result by passing job id to RAWExtractionResults URL
        _getResultURL = str("https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults(\'" + _jobID + "\')/$value")
        print('Retrieve result from' + _getResultURL)
        resp = requests.get(_getResultURL, headers=_header, stream=True)

        # write output to file
        output_file = str(_outputPath + _outputName + str(os.getpid()) + '.csv.gz')
        with open(output_file, 'wb') as f:
            f.write(resp.raw.read())

        print('Write output to ' + output_file + 'completed\n\n')
        print('Below is sample data from ' + output_file)
        # read data from csv.gz and shows output from dataframe head() and tail()
        df = pd.read_csv(output_file, compression='gzip')
        print(df.head())
        print('....')
        print(df.tail)

    except Exception as ex:
        print('Exception occurs: ', ex)

    return


