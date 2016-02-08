#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Database Provisioning Script
******************************************************

"""
# Import standard Python packages and read outfile
import getopt
import sys
import threading

sys.path.insert(0, r'\\qafile2\TS\Working Data\Shashank\Validation Library\ValidationLib')
import datetime
import warnings

warnings.filterwarnings('ignore')

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print ("Outfile: " + OUTFILE)
# OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\provisioning.csv'
if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging
import pandas as pd

from ValidationLib.database.main import Database

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HANDLER_INFO = logging.FileHandler((OUTFILE[:-4] + '-info.log'))
HANDLER_INFO.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER_INFO)

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'


def file_skeleton(outfile):
    pd.DataFrame(columns=['OutputLocation', 'Status']).to_csv(outfile, index=False)


# Extract the given arguments
try:
    server = 'QAWUDB2\SQL2012'
    output_location = r'\\qafile2\TS\Working Data\Shashank\backup'
    LOGGER.info(output_location)
except:
    LOGGER.error('Please verify the inputs')
    file_skeleton(OUTFILE)
    sys.exit()

if __name__ == "__main__":

    try:
        start = time.time()

        LOGGER.info('********************************')
        LOGGER.info('**      Touchstone v.3.0      **')
        LOGGER.info('********************************')

        LOGGER.info('\n********** Log header **********\n')
        LOGGER.info('Description:   Import Log Validation')
        LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
        LOGGER.info('Status:                Completed')

        db = Database(server)
        database_list = db.get_database_list()
        backup_database = []

        excluded_database = ['master', 'model', 'msdb', 'tempdb', 'ReportServer', 'ReportServerTempDB', 'AIRPSOLD',
                             'AIRGeography', 'AIRSpatial', 'AIRReference', 'AIRIndustry', 'AIRDQIndustry', 'AIRMap',
                             'AIRMapBoundary', 'AIRDBAdmin', 'AIRAddressServer', 'ReportServer$SQL2012',
                             'ReportServer$SQL2012TempDB', 'HPCDiagnostics', 'HPCManagement', 'HPCMonitoring',
                             'HPCReporting', 'HPCScheduler', 'ReportServer', 'ReportServerTempDB',
                             'AIRPropertyExposure', 'AIRLossCost']
        for i in range(len(database_list)):
            if database_list[i] not in excluded_database:
                backup_database.append(str(database_list[i]))

        thread_list = []
        for database in (backup_database[:5]):
            t = threading.Thread(target=db.backup_db, args=(database, output_location, server,))
            thread_list.append(t)

        for thread in thread_list:
            thread.start()

        for thread in thread_list:
            thread.join()

        output = pd.DataFrame()
        output.to_csv(OUTFILE, index=False)
        LOGGER.info('----------------------------------------------------------------------------------')
        LOGGER.info('              Import Log Completed Successfully                            ')
        LOGGER.info('----------------------------------------------------------------------------------')

        LOGGER.info('********** Process Complete Time: ' + str(time.time() - start) + ' Seconds **********')

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
        file_skeleton(OUTFILE)
        sys.exit()
