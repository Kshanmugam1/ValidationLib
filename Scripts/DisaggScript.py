#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Disaggregation Validation Script
******************************************************

"""

# Import standard Python packages and read outfile
import getopt
import warnings
import time
import logging
import sys

sys.path.insert(0, r'\\qafile2\TS\Working Data\Shashank\Validation Library\ValidationLib')
import copy

import datetime
import pandas as pd



# Import internal packages
from ValidationLib.analysis.main import Disaggregation
from ValidationLib.database.main import Database
from ValidationLib.general.main import set_column_sequence

warnings.filterwarnings('ignore')

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a

# OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\Disaggregation.csv'
if OUTFILE is None:
    LOGGER.error('OUTFILE not passed')
    sys.exit()


HANDLER_INF0 = logging.FileHandler(OUTFILE[:-4] + '-info.log')
HANDLER_INF0.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER_INF0)


__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10 | Anaconda 2.3.0 (64-bit)'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'


def file_skeleton(outfile):

    pd.DataFrame(columns=['ContractID', 'LocationID', 'GeographySID', 'Latitude', 'Longitude',
                          'CountryCode', 'ExchangeRate', 'GeoCoding',
                          'Resolution', 'LocationTypeCode', 'ChildLocationCount',
                          'fltWeight', 'dblMinCovA', 'dblMinCovB', 'dblMinCovC',
                          'dblMinCovD', 'ReplacementValueA',
                          'ReplacementValueB', 'ReplacementValueC',
                          'ReplacementValueD', 'CalcReplacementValueA',
                          'CalcReplacementValueB', 'CalcReplacementValueC',
                          'CalcReplacementValueD', 'Status']).to_csv(outfile, index=False)

# Extract the given arguments
try:
    server = sys.argv[3]
    result_db = sys.argv[4]
    exposure_db = sys.argv[5]
    analysis_name = sys.argv[6]
    exposure_name = sys.argv[7]

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
        LOGGER.info('Description:   Disaggregation Validation')
        LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
        LOGGER.info('Status:                Completed')

        LOGGER.info('\n********** Log Import Options **********\n')
        # Initialize the connection with the server
        try:
            db = Database(server)
            disaggregation = Disaggregation(server)
            LOGGER.info('Server: ' + str(server))
        except:
            LOGGER.error('Error: Check connection to database server')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            loc_info = db.location_info_disagg(exposure_db, exposure_name)
            loc_info['ExchangeRate_Original'] = copy.deepcopy(loc_info['ExchangeRate'])
            loc_info.loc[loc_info['CurrencyCode'] == 'USD', 'ExchangeRate'] = 1.0
            LOGGER.info('Exposure DB: ' + str(exposure_db))
            LOGGER.info('Exposure Name: ' + str(exposure_name))
        except:
            LOGGER.error('Error: Check the exposure db or exposure name')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            analysis_sid = db.analysis_sid(analysis_name)
            LOGGER.info('Analysis SID: ' + str(analysis_sid))
        except:
            LOGGER.error('Error: Failed to extract the analysis SID from analysis name')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            staging_output = disaggregation.staging_table(analysis_sid, exposure_db)
            staging_output.to_csv(OUTFILE[:-4] + '-StagingTable.csv')
        except:
            LOGGER.error('Error: Failed to extract staging locations table')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            resultDF = disaggregation.calc_disaggregation(loc_info, staging_output)
        except:
            LOGGER.error('Error: Failed to disaggregate locations')
            file_skeleton(OUTFILE)
            sys.exit()

        sequence = ['ContractID', 'LocationID', 'GeographySID', 'Latitude', 'Longitude',
                    'CountryCode', 'ExchangeRate', 'GeoCoding',
                    'Resolution', 'LocationTypeCode', 'ChildLocationCount',
                    'fltWeight', 'dblMinCovA', 'dblMinCovB', 'dblMinCovC',
                    'dblMinCovD', 'ReplacementValueA',
                    'ReplacementValueB', 'ReplacementValueC',
                    'ReplacementValueD', 'CalcReplacementValueA',
                    'CalcReplacementValueB', 'CalcReplacementValueC',
                    'CalcReplacementValueD', 'Status']
        resultDF = set_column_sequence(resultDF, sequence)
        resultDF.to_csv(OUTFILE, index=False)

        LOGGER.info('---------------------------------------------------------------------------')
        LOGGER.info('         Disaggregation Validation Completed Successfully                  ')
        LOGGER.info('---------------------------------------------------------------------------')

        LOGGER.info('Process Complete Time: ' + str(time.time() - start) + ' Seconds ')

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
        file_skeleton(OUTFILE)
        sys.exit()