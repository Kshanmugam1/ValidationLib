#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Financial Terms Validation Script
******************************************************

"""

# Import standard Python packages and read outfile
import getopt
import warnings
import time
import logging
import sys

sys.path.insert(0, r'\\qafile2\TS\Working Data\Shashank\Validation Library\ValidationLib')

import datetime
import pandas as pd



# Import internal packages
from ValidationLib.financials.main import FinancialTerms
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

# OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\FinancialTerms.csv'
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
    pd.DataFrame(columns=['ContractID', 'LocationID', 'AIROccupancyCode', 'ReplacementValueA',
                          'ReplacementValueB', 'ReplacementValueC',
                          'ReplacementValueD', 'DamageRatio', 'LimitTypeCode', 'Limit1',
                          'Limit2', 'Limit3', 'Limit4', 'DeductibleTypeCode', 'Deductible1', 'Deductible2',
                          'Deductible3', 'Deductible4', 'Participation1', 'Participation2', 'exposedGroundUp',
                          'exposedGross', 'expectedGross', 'Difference', 'Status']).to_csv(outfile, index=False)


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

        ft = FinancialTerms()

        # Initialize the connection with the server
        try:
            db = Database(server)
            LOGGER.info('Server: ' + str(server))
        except:
            LOGGER.error('Error: Check connection to database server')
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
            result_sid = db.result_sid(analysis_sid)
            LOGGER.info('Mod Result SID: ' + str(result_sid))
        except:
            LOGGER.error('Error: Failed to extract result SID from analysis SID')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            location_information = db.get_loc_info_policy(exposure_db, exposure_name, result_db, result_sid)
            LOGGER.info('Location Information:\n ' + str(location_information))
        except:
            LOGGER.error('Error: Failed to extract location information')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            resultDF = ft.apply_terms(location_information)
            resultDF['Difference'] = resultDF['expectedGross'] - resultDF['exposedGross']
        except:
            LOGGER.error('Error: Failed to apply financial terms')
            file_skeleton(OUTFILE)
            sys.exit()

        sequence = ['ContractID', 'LocationID', 'AIROccupancyCode', 'ReplacementValueA',
                    'ReplacementValueB', 'ReplacementValueC',
                    'ReplacementValueD', 'DamageRatio', 'LimitTypeCode', 'Limit1',
                    'Limit2', 'Limit3', 'Limit4', 'DeductibleTypeCode', 'Deductible1', 'Deductible2',
                    'Deductible3', 'Deductible4', 'Participation1', 'Participation2', 'exposedGroundUp',
                    'exposedGross', 'expectedGross', 'Difference', 'Status']

        resultDF = set_column_sequence(resultDF, sequence)
        resultDF.to_csv(OUTFILE, index=False)

        LOGGER.info('---------------------------------------------------------------------------')
        LOGGER.info('         Financial Terms Validation Completed Successfully                 ')
        LOGGER.info('---------------------------------------------------------------------------')

        LOGGER.info('Process Complete Time: ' + str(time.time() - start) + ' Seconds ')

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
        file_skeleton(OUTFILE)
        sys.exit()
