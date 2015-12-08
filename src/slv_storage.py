# encoding: utf-8
# module slv_connector
from datetime import datetime, timedelta
import pandas as pd
import os


class FileStorage(object):
    """ Handles storage of SLV data in files on the local file system

    Attribues:
        directory: A string containing a path to the directory we'll read and write data from
    """

    def __init__(self, directory, fromdate, todate, cron):
        """ Constructor allows for directory to be passed in """
        self.directory = directory
        self.fromdate = fromdate
        self.todate = todate
        self.cron = cron

    def get_existing_data(self):
        """ Read in any file in the specified directory that contain data from the previous 24 hours

        :return: A pandas dataframe containing all the data found in the previous 24 hours
        """
        # Find the format string for the earliest and latest data we want to retrieve
        now = datetime.now()
        end = now - timedelta(minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
        start = end - timedelta(hours=24)
        endstr = end.strftime("%Y%m%d%H%M%S")
        startstr = start.strftime("%Y%m%d%H%M%S")

        # Prepare the input files
        frames = []
        for filename in os.listdir(self.directory):
            if filename.startswith("slv") and filename.endswith(".csv"):
                name = filename.split('.')[0]
                filestart = name.split('-')[1]
                fileend = name.split('-')[2]
                if (int(fileend) > int(startstr)) and (int(filestart) < int(endstr)):
                    # print 'file in correct range:', filename
                    frame = pd.read_csv(self.directory + filename)
                    frames.append(frame)
                    # else:
                    #     print 'file outside correct range:', filename
        if len(frames) == 0:
            return None

        df = pd.concat(frames, ignore_index=True)
        df = df[frames[0].columns]
        print 'Number of records found in existing data: ' + str(df.shape[0])

        return df

    def try_write(self, df, start, end):
        """ Attempt to write the dataframe to the specified filename

        :param df: The dataframe to attempt to write
        :param start: the start date
        :param end: the end date
        :return: nothing
        """
        filename = 'slv-' + start.strftime("%Y%m%d%H%M%S") + '-' + end.strftime("%Y%m%d%H%M%S") + '.csv'
        try:
            df.to_csv(self.directory + filename, index=False)
        except:
            print 'the file could not be saved to the specified directory, saving in the current directory instead'
            df.to_csv(self.directory + filename, index=False)

    def write(self, df):
        """ If we're running in cron-mode - split the dataframe into hours and save a separate file for each hour.
        Otherwise, save the whole dataframe in a file named using the start and end dates

        :param df: A pandas data frame containing all the data to write
        :return: nothing
        """
        print 'Writing to file(s)...'
        if not self.cron:
            self.try_write(df, self.fromdate, self.todate)
        else:
            end = self.todate
            for h in range(24):
                start = end - timedelta(hours=1)
                hourdf = df[(df.eventTime >= start.strftime("%Y-%m-%d %H:%M:%S")) & (
                    df.eventTime < end.strftime("%Y-%m-%d %H:%M:%S"))]
                if hourdf.shape[0] > 0:
                    # print 'writing file in range:', start.strftime("%Y-%m-%d %H:%M:%S"), ":", end.strftime("%Y-%m-%d %H:%M:%S")
                    self.try_write(hourdf, start, end)
                end = start


class CloudSQLStorage(object):
    """Handles storage of SLV data in a CloudSQL instance

    Attributes:
        connectionstring: A string used to connect to the CloudSQL instance
    """

    def __init__(self, configfile):
        self.config = configfile

    # Method to read a particular date range and return a pandas dataframe

    # Method to take a pandas dataframe and write the data to CloudSQL

    def __init__(self):
        pass
