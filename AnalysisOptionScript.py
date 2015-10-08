#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Analysis Option Validation Script
******************************************************

"""

# Import standard Python packages and read outfile
import getopt
import warnings
import time
import logging
import sys

import datetime
import pandas as pd


# Import internal packages
from ValidationLib.database.main import Database

warnings.filterwarnings('ignore')

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a

# OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\AnalysisOption.csv'

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
__status__ = 'Production'


def file_skeleton(outfile):

    pd.DataFrame(columns=['AnalysisTypeCode', 'SourceTemplateName', 'EventSet', 'PerilSet',
                          'EventFilter', 'DemandSurgeOptionCode', 'EnableCorrelation',
                          'ApplyDisaggregation', 'AveragePropertyOptionCode', 'RemapConstructionOccupancy',
                          'LossModOption', 'MoveMarineCraftGeocodesToCoast', 'SaveGroundUp',
                          'SaveRetained', 'SavePreLayerGross',
                          'SaveGross', 'SaveNetOfPreCAT',
                          'SavePostCATNet', 'OutputType', 'SaveCoverage', 'SaveClaims',
                          'SaveInjury', 'SaveMAOL', 'BaseAnalysisSID']).to_csv(outfile, index=False)


# Extract the given arguments
try:
    server = sys.argv[3]
    analysis_name = sys.argv[4]
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
        LOGGER.info('Description:   Analysis Option Validation')
        LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
        LOGGER.info('Status:                Completed')

        LOGGER.info('\n********** Log Import Options **********\n')
        # Initialize the connection with the server
        try:
            db = Database(server)
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
            analysis_information = db.analysis_option(analysis_sid)
        except:
            LOGGER.error('Error: Unable to fetch analysis option details')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            if analysis_information['AnalysisTypeCode'].values[0] == 'LGRP' or 'IMPACT':
                get_analysis = db.get_analysis(analysis_sid)
                analyses = get_analysis.iloc[:, 0].values
                for i in range (len(analyses)):
                    analysis_sid = db.analysis_sid(analyses[i])
                    analysis_information = pd.concat([analysis_information, db.analysis_option(analysis_sid)], axis=0)
        except:
            LOGGER.error('Error: Failed to append secondary analysis')
            file_skeleton(OUTFILE)
            sys.exit()

        analysis_information.to_csv(OUTFILE, index=False)

        LOGGER.info('---------------------------------------------------------------------------')
        LOGGER.info('         Analysis Option Validation Completed Successfully                  ')
        LOGGER.info('---------------------------------------------------------------------------')

        LOGGER.info('Process Complete Time: ' + str(time.time() - start) + ' Seconds ')

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
        file_skeleton(OUTFILE)
        sys.exit()

