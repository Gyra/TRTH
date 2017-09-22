# -*- coding: UTF-8 -*-
"""
This is used to request data from DSS TRTH
Adapted from official example and document:
https://developers.thomsonreuters.com/thomson-reuters-tick-history-trth/thomson-reuters-tick-history-trth-rest-api/learning?content=11220&type=learning_material_item
https://github.com/TR-API-Samples/Article.TRTH.Python.REST.OndemandRequestTimeAndSales/blob/master/TickHistoryTimesAndSalesRequest.py
current version: Handing Sun 21/9/2017
"""

import os
import json
import requests
from time import sleep
import pandas as pd
from getpass import _raw_input, getpass, GetPassWarning
import collections

_outputPath = '/Users/gyra/Dropbox (Personal)/Python/TRTH/TRTH/output/'
_outputName = 'trthTest'
_retryTime = 30.0
_jsonFileName = 'trth_request_test.json'


def requestNewToken(uid='', password=''):
    _authenURL = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken'
    _header = {}
    _header['Prefer'] = 'respond-async'
    _header['Content-Type'] = 'application/json; odata.metadata=minimal'  # CHECK MEANING
    _data = {'Credentials': {
        'Password': password,
        'Username': uid
        }
    }

    # send login request
    resp = requests.post(_authenURL, json=_data, headers=_header)

    # error
    if resp.status_code != 200:
        raise Exception('Authentication Error Status Code: ' + str(resp.status_code) + '. Message: ' + json.dumps(json.loads(resp.text), indent=4))

    return json.loads(resp.text)['value']


def extractRaw(token, json_payload):
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
            print('Get status from ' + str(_location))

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
        _jobID = json_resp.get('JobId')
        print('Status is completed and the JobID is ' + str(_jobID) + '\n')

        # check if the response contains Notes. If the note exists print it to console
        if len(json_resp.get('Notes')) > 0:
            print('Note:\n================================')
            for var in json_resp.get('Notes'):
                print(var)
            print('================================\n')

        # get the result by passing job id to RAWExtractionResults URL
        _getResultURL = str("https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults(\'" + _jobID + "\')/$value")
        print('Retrieve result from ' + _getResultURL)
        resp = requests.get(_getResultURL, headers=_header, stream=True)

        # write output to file
        output_file = str(_outputPath + _outputName + str(os.getpid()) + '.csv.gz')
        with open(output_file, 'wb') as f:
            f.write(resp.raw.read())

        print('Write output to ' + output_file + ' completed\n\n')
        print('Below is sample data from ' + output_file)
        # read data from csv.gz and shows output from dataframe head() and tail()
        df = pd.read_csv(output_file, compression='gzip')
        print(df.head())
        print('....')
        print(df.tail)

    except Exception as ex:
        print('Exception occurs: ', ex)

    return


if __name__ == '__main__':
    try:
        # request a new token
        print('Login to DSS')
        _DSSusername = _raw_input('Enter DSS Username:')
        try:
            _DSSpassword = getpass(prompt='Enter DSS Password')
            _token = requestNewToken(_DSSusername, _DSSpassword)
        except GetPassWarning as e:
            print(e)
        print('Token = ' + _token + '\n')

        #read the http request body from json file.
        with open(_jsonFileName, 'r') as fd:
            query_string = json.load(fd, object_pairs_hook=collections.OrderedDict)
        extractRaw(_token, query_string)

    except Exception as e:
        print(e)



# a = collections.OrderedDict({
#     "ExtractionRequest": collections.OrderedDict({
#         "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.TickHistoryIntradaySummariesExtractionRequest",
#         "ContentFieldNames": ["Close Bid"],
#         "IdentifierList": collections.OrderedDict({
#             "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
#             "InstrumentIdentifiers": [{
#                 "Identifier": "EUR=",
#                 "IdentifierType": "Ric"
#             }
#             ]
#         }),
#         "Condition": {
#             "MessageTimeStampIn": "GmtUtc",
#             "ReportDateRangeType": "Range",
#             "QueryStartDate": "2017-09-03T23:45:00.000Z",
#             "QueryEndDate": "2017-09-06T12:30:00.000Z"
#             # "DisplaySourceRIC": True
#         }
#     })
# })
# with open('trth_request_test.json', 'w') as ff:
#     ff.write(json.dumps(a, indent=4, sort_keys=False, ensure_ascii=False))
