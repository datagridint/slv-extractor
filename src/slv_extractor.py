# -*- coding: utf-8 -*-
"""
SLV Extractor

This script will connect to DataGrid's SLV site through the SLV API and download all data that hasn't yet been extracted
The resulting data will be stored in .csv files or Google CloudSQL
"""

import getopt
import json
import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import requests
import requests.packages.urllib3
from slv_storage import CloudSQLStorage
import warnings

# Silence FutureWarnings
warnings.simplefilter(action="ignore", category=FutureWarning)

# Silence the SSL warnings
requests.packages.urllib3.disable_warnings()

# The credentials to login to SLV
CREDENTIALS = {'j_username': 'amko', 'j_password': '2jrirgdi'}

# The list of values to retrieve from SLV. This is the subset we need to match the existing data set for previous
# Tableau work. Retrieving all historical data from SLV is very slow, so this should help to speed things up
SIMPLEVALUES = [
    'Temperature',
    'RunningHoursLamp',
    'Energy',
    'Current',
    'MainVoltage',
    'MeteredPower',
    'PowerFactor'
]

# Endpoints
GETPROFILPROPERTIES_URL = "https://mycityisgreen.com/reports/api/servlet/SLVAssetAPI?methodName=getProfilProperties&ser=json"
SECURITY_URL = "https://mycityisgreen.com/reports/j_security_check"
GETGEOZONEDEVICES_URL = "https://mycityisgreen.com/reports/api/servlet/SLVAssetAPI?methodName=getGeoZoneDevices&ser=json"
GETDEVICESLOGVALUES_URL = "https://mycityisgreen.com/reports/api/servlet/SLVLoggingAPI?methodName=getDevicesLogValues&ser=json"

# Defaults for command line arguments that control the script, especially in regards to cron-mode vs. standalone mode
directory = "../data/"
todate = datetime.now()
todate = todate - timedelta(minutes=todate.minute, seconds=todate.second, microseconds=todate.microsecond)
fromdate = todate - timedelta(hours=24)
cron = True


def get_args(argv):
    """ Read the command-line arguments and validate them

    :param argv: The command line arguments
    :return: Nothing.
    """
    global fromdate, cron
    d, f, t = '', '', ''

    try:
        opts, args = getopt.getopt(argv, "hd:f:t:", ["directory=", "fromdate=", "todate="])
    except getopt.GetoptError:
        print 'slv-extractor.py -d <directory> -f <fromdate (YY/mm/dd HH:MM:SS)> -t <todate (YYYY/mm/dd HH:MM:SS)>'
        exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'slv-extractor.py -d <directory> -f <fromdate (YY/mm/dd HH:MM:SS)> -t <todate (YYYY/mm/dd HH:MM:SS)>'
        elif opt in ("-d", "--directory"):
            d = arg
        elif opt in ("-f", "--fromdate"):
            f = arg
        elif opt in ("-t", "--todate"):
            t = arg

    # Now let's validate the input and exit if anything is bad
    validate_input(d, f, t)

    # If the to date is present, we'll assume they want everything up to that date (starting in 2015)
    if t and not f:
        fromdate = datetime.strptime('2015/01/01 00:00:00', "%Y/%m/%d %H:%M:%S")

    # Let's set the global variables depending on which mode we're in
    if f or t:
        cron = False


###
# Make sure the command line argument values we've read are valid
def validate_input(d, f, t):
    global fromdate, todate

    # Validate the directory either exists or can be created
    if d:
        try:
            path = os.path.dirname(d)
            if not os.path.exists(path):
                os.makedirs(path)
        except:
            print 'Directory does not exist and could not be created. Correct directory name, or leave blank to save file in current directory'
            exit()
    # Validate "From" argument
    if f:
        try:
            fromdate = datetime.strptime(f, "%Y/%m/%d %H:%M:%S")
        except:
            print 'Invalid fromDate:', f, '- Must be in "YYYY/mm/dd HH:MM:SS" format'
            exit()
    # Validate "To" argument
    if t:
        try:
            todate = datetime.strptime(t, "%Y/%m/%d %H:%M:%S")
        except:
            print 'Invalid toDate:', t, '- Must be in "YYYY/mm/dd HH:MM:SS" format'
            exit()
    # Validate that From is before To
    if f and t:
        if todate < fromdate:
            print 'The From date must be before the To date'
            exit()


def login():
    """ Perform authentication and return the cookie """
    # Initial request will return a cookie with the JSESSIONID value
    r = requests.get(GETPROFILPROPERTIES_URL)
    cookiejar = r.cookies

    # Now send that cookie to the security check page to perform auth
    r = requests.post(SECURITY_URL, data=CREDENTIALS, cookies=cookiejar)
    cookiejar = r.cookies
    #     cookies = requests.utils.dict_from_cookiejar(cookiejar)
    #     print cookies

    return cookiejar


def get_geozonerootid(cookiejar):
    """ Make a request to the user's profile, and extract the geoZone out of the returned XML

    :param cookiejar: contains the session ID needed for SLV's API
    """
    r = requests.get(GETPROFILPROPERTIES_URL, cookies=cookiejar)
    data = json.loads(r.text)
    for param in data:
        if param['key'] == 'geoZoneRootId':
            print 'found geoZoneRootId:', param['value']
            return param['value']
    print 'GetZoneRootId not present in the profile. This is an unrecoverable error, exiting...'
    exit()


def get_devices(geozone_id, cookiejar):
    """ Get all the devices for the provided geoZone

    :param geozone_id: The root ID
    :param cookiejar: contains the session ID needed for SLV's API
    """
    # Retrieve the devices data (and time it)
    a = datetime.now()
    r = requests.post(GETGEOZONEDEVICES_URL, data={'geoZoneId': geozone_id, 'recurse': 'True'}, cookies=cookiejar)
    b = datetime.now()
    c = b - a
    print 'time for getDevicesLogValues request:', c.total_seconds()

    # Convert the json response to a dataframe
    df = pd.read_json(r.text)

    # Only keep certain columns
    df = df[['id', 'geoZoneNamesPath', 'categoryStrId', 'idOnController', 'name']]

    # Only keep the streetlight devices (ignore camera, controller, and smart meters)
    df = df[df.categoryStrId == 'streetlight']

    return df


def get_readings(device_ids, cookiejar):
    """Obtain the readings for the specified list of devices from the getDevicesLogValues endpoint. If the dates are
    provided, use them. Otherwise, just get the last 24 hours of data

    :param device_ids: an array-like containing the IDs of the devices to retrieve from SLV
    :param cookiejar: contains the session ID needed for SLV's API
    :return: A pandas dataframe containing the data returned from SLV, sorted by time, id, then field
    :rtype: dataframe
    """
    readings_df = None

    # fromdate and todate contain the total range of time we want. However, we dont' want everything
    # at once as it can take a LONG time (~90 seconds per day) so let's just do one day at a time
    end = todate - timedelta(seconds=1)

    while end > fromdate:
        start = end - timedelta(days=1)
        if start < fromdate:
            start = fromdate

        data = {'deviceId': device_ids.values, 'from': start.strftime("%Y-%m-%d %H:%M:%S"),
                'to': end.strftime("%Y-%m-%d %H:%M:%S"), 'name': SIMPLEVALUES}
        print 'Retrieving device data from', start.strftime("%Y-%m-%d %H:%M:%S"), 'to', end.strftime(
            "%Y-%m-%d %H:%M:%S") + '...'
        a = datetime.now()
        r = requests.post(GETDEVICESLOGVALUES_URL, data=data, cookies=cookiejar)
        b = datetime.now()
        c = b - a
        print 'time for getDevicesLogValues request:', c.total_seconds()

        if r.status_code != 200:
            print "There was an error retrieving values in range:", fromdate, "-", todate
            exit()

        df = pd.read_json(r.text)
        print df.columns.values
        if not df.empty:
            df = df[['deviceId', 'eventTime', 'updateTime', 'name', 'value', 'status']].rename(columns={'deviceId': 'id', 'name': 'field'})
            #         print df

            if readings_df is None:
                readings_df = df
            else:
                readings_df = pd.concat([readings_df, df], axis=0)

        end = start

    if readings_df is not None:
        readings_df = readings_df.sort(['eventTime', 'id', 'field'])

    return readings_df


def main(argv):
    """
    The extraction script can run in two modes:
      - setup the script on a cron job to pull data every hour
      - run the script standalone with a date range to pull a specific date range of data

    :param argv: command line arguments
    """
    # Validate and set the global variables based on the command-line arguments provided
    get_args(argv)

    # Perform auth and get the cookie
    cookiejar = login()

    # Determine the root geozone
    geozone_id = get_geozonerootid(cookiejar)

    # Get all the streetlight devices nested under the root geozone
    devices_df = get_devices(geozone_id, cookiejar)

    # Get all the readings from these devices
    readings_df = get_readings(devices_df.id, cookiejar)
    if readings_df is None:
        print "No readings retrieved from SLV in this time range, exiting..."
        exit()

    # Merge the readings into the device dataframe
    df = readings_df.merge(devices_df, on='id', how='outer')

    # Reorder the columns and select only the ones we want
    df = df[['geoZoneNamesPath', 'name', 'eventTime', 'updateTime', 'field', 'value']]

    # Drop the rows that don't have any data (not all streetlights will have data for the requested dates)
    df.dropna(inplace=True)

    # Now, convert into the following format, which matches the previously used format (plus the geozone data):
    # geoZoneNamesPath, eventTime, name, temperature, RunningHoursLamp, Energy, Current, MainVoltage, MeteredPower, PowerFactor
    df2 = df[df.field == 'Temperature'][['geoZoneNamesPath', 'eventTime', 'updateTime', 'name', 'value']].rename(
        columns={'value': 'Temperature'})
    df2 = df2.merge(
        df[df.field == 'RunningHoursLamp'][['geoZoneNamesPath', 'eventTime', 'updateTime', 'name', 'value']],
        on=['geoZoneNamesPath', 'eventTime', 'updateTime', 'name'], how='outer').rename(
        columns={'value': 'RunningHoursLamp'})
    df2 = df2.merge(df[df.field == 'Energy'][['geoZoneNamesPath', 'eventTime', 'updateTime', 'name', 'value']],
                    on=['geoZoneNamesPath', 'eventTime', 'updateTime', 'name'], how='outer').rename(
        columns={'value': 'Energy'})
    df2 = df2.merge(df[df.field == 'Current'][['geoZoneNamesPath', 'eventTime', 'updateTime', 'name', 'value']],
                    on=['geoZoneNamesPath', 'eventTime', 'updateTime', 'name'], how='outer').rename(
        columns={'value': 'Current'})
    df2 = df2.merge(df[df.field == 'MainVoltage'][['geoZoneNamesPath', 'eventTime', 'updateTime', 'name', 'value']],
                    on=['geoZoneNamesPath', 'eventTime', 'updateTime', 'name'], how='outer').rename(
        columns={'value': 'MainVoltage'})
    df2 = df2.merge(df[df.field == 'MeteredPower'][['geoZoneNamesPath', 'eventTime', 'updateTime', 'name', 'value']],
                    on=['geoZoneNamesPath', 'eventTime', 'updateTime', 'name'], how='outer').rename(
        columns={'value': 'MeteredPower'})
    df2 = df2.merge(df[df.field == 'PowerFactor'][['geoZoneNamesPath', 'eventTime', 'updateTime', 'name', 'value']],
                    on=['geoZoneNamesPath', 'eventTime', 'updateTime', 'name'], how='outer').rename(
        columns={'value': 'PowerFactor'})
    print "Number of records retrieved in this request:", df2.shape[0]

    # Create a connector to read and write slv data to the file system
    # store = FileStorage(directory, fromdate, todate, cron)
    store = CloudSQLStorage(fromdate, todate)

    existing_df = store.get_existing_data()
    # print existing_df

    if existing_df is not None:
        # merge the current readings with what we found in storage, and then drop duplicates
        df2[['eventTime', 'updateTime']] = df[['eventTime', 'updateTime']].apply(pd.to_datetime)
        df2 = pd.concat([existing_df, df2], ignore_index=True)
        print 'Number of records after merging requested and existing data:', df2.shape[0]
        df2.drop_duplicates(subset=['geoZoneNamesPath', 'name', 'eventTime'], inplace=True)
        print 'Number of records after deduping merged data:', df2.shape[0]
    else:
        print 'Number of records found in this date range:', df2.shape[0]

    # Sort and swap the columns so that they're easy to look at in Excel
    df = df2.sort(['geoZoneNamesPath', 'name', 'eventTime'])
    df = df[['geoZoneNamesPath', 'name', 'eventTime', 'updateTime', 'Temperature', 'RunningHoursLamp', 'Energy', 'Current', 'MainVoltage', 'MeteredPower', 'PowerFactor']]

    # Write the data to the store
    store.write(df)

    print "Done"


if __name__ == "__main__":
    print 'Extracting data from SLV API at: ' + str(datetime.now()) + '...'
    main(sys.argv[1:])

# This is the full set of all possible variables that can be extracted from SLV. Many/most of these variables have no
# values in SLV at this time, but they may be useful later.
ALLVALUES = [
    'RSSI',
    'BallastFailure',
    'ControllerFailure',
    'CycleCount',
    'DefaultLostNode',
    'BallastTemp',
    'FlickerCount',
    'FlickeringFailure',
    'HighVoltage',
    'HighCurrent',
    'HighOLCTemperature',
    'brandId',
    'RunningHoursLamp',
    'LampCurrent',
    'Energy',
    'LampCommandLevel',
    'LampLevel',
    'LampCommandSwitch',
    'LampSwitch',
    'LampVoltage',
    'power',
    'LampFailure',
    'LowVoltage',
    'LowCurrent',
    'LowPowerFactor',
    'LuxLevel',
    'Current',
    'MainVoltage',
    'MeteredPower',
    'PowerFactor',
    'RelayFailure',
    'TalqAddress',
    'Temperature',
    'modelFunctionId',
    'MacAddress'
]
