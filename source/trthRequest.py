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

_outputPath = './output'
_outputName = './trthTest'
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

