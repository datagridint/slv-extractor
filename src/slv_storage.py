# encoding: utf-8
# module slv_connector
from datetime import datetime, timedelta
import pandas as pd
import os
from sqlalchemy import create_engine


class FileStorage(object):
    """ Handles storage of SLV data in files on the local file system

    Attribues:
        directory: A string containing a path to the directory we'll read and write data from
    """

    def __init__(self, directory, fromdate, todate, cron):
        """ Constructor allows for directory, dates, and run-mode to be passed in """
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
        No attributes
    """
    config = {
        'user': 'slvdbuser',
        'password': 'slvdbuser',
        'host': '173.194.249.226',
        'port': '3306',
        'database': 'streetlightdata'
    }
    mysql_connectionstring = 'mysql://{user}:{password}@{host}:{port}/{database}'.format(**config)

    def __init__(self, fromdate, todate):
        self.cnx = create_engine(self.mysql_connectionstring, pool_recycle=1800)
        # self.cnx = mysql.connector.connect(**self.config)
        self.fromdate = fromdate
        self.todate = todate

    def get_existing_data(self):
        """ Read in any file in the specified directory that contain data from the current specified date range

        :return: A pandas dataframe containing all the data found in the previous 24 hours
        """
        sql = '''
            SELECT *
              FROM readings
             WHERE eventtime >= '{0}'
               AND eventtime <= '{1}'
        '''.format(self.fromdate, self.todate)

        df = pd.read_sql(sql=sql, con=self.cnx)
        return df

    def write(self, df):
        """ Takes a pandas dataframe and writes the data to CloudSQL

        :param df: A dataframe of readings data
        :return: Nothing
        """
        # Clear the un-merged data
        del_sql = '''
            DELETE FROM readings
             WHERE eventtime >= '{0}'
               AND eventtime <= '{1}'
        '''.format(self.fromdate, self.todate)
        self.cnx.execute(del_sql)

        # Now write the new merged data back in
        df.to_sql(name='readings', con=self.cnx, flavor='mysql', if_exists='append', index=False)
