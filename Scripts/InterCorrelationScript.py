#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Inter Correlation Validation Script
******************************************************

"""

# Import standard Python packages and read outfile
import getopt
import sys
import datetime
import warnings

warnings.filterwarnings('ignore')

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print ("Outfile: " + OUTFILE)

if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HANDLER_INF0 = logging.FileHandler(OUTFILE[:-4] + '-info.log')
HANDLER_INF0.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER_INF0)

# Import internal packages
from ValidationLib.general.main import *
from ValidationLib.database.main import *
from ValidationLib.financials.main import *


__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10 |Anaconda 2.3.0 (64-bit)'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'


def file_skeleton(outfile):

    pd.DataFrame(columns=['CatalogTypeCode', 'ModelCode', 'PortGuSD', 'CalculatedPortGuSD',
                          'DifferencePortGuSD_Percent', 'PortGrSD', 'CalculatedPortGrSD',
                          'DifferencePortGrSD_Percent', 'Status']).to_csv(outfile, index=False)

# Extract the given arguments
try:
    server = sys.argv[3]
    result_db = sys.argv[4]
    contract_analysis_name = sys.argv[5]
    tolerance = sys.argv[6]
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
        LOGGER.info('Description:   Inter Correlation Validation')
        LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
        LOGGER.info('Status:                Completed')

        LOGGER.info('\n********** Log Import Options **********\n')
        # Initialize the connection with the server
        try:
            db = Database(server)
            inter_correlation = Correlation(server)
            LOGGER.info('Server: ' + str(server))
        except:
            LOGGER.error('Error: Check connection to database server')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            contract_analysis_sid = db.analysis_sid(contract_analysis_name)
            LOGGER.info('Analysis SID: ' + str(contract_analysis_sid))
        except:
            LOGGER.error('Error: Failed to extract the contract analysis SID from contract analysis name')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            intra_correlation_fac, inter_correlation_fac = inter_correlation.correlation_factor(contract_analysis_sid)
            LOGGER.info('Intra Correlation Factor: ' + str(intra_correlation_fac))
            LOGGER.info('Inter Correlation Factor: ' + str(inter_correlation_fac))
        except:
            LOGGER.error('Error: Failed to fetch the correlation factors')
            file_skeleton(OUTFILE)
            sys.exit()

        try:
            contractResultSID = db.result_sid(contract_analysis_sid)
            LOGGER.info('Result SID: ' + str(contractResultSID))
        except:
            LOGGER.error('Error: Failed to get contract result SID from analysis SID')
            file_skeleton(OUTFILE)
            sys.exit()

        try:

            resultDF_detailed, resultDF_summary = inter_correlation.loss_sd(contractResultSID, result_db, 'Inter',
                                                                            inter_correlation=inter_correlation_fac,
                                                                            tolerance=tolerance)
        except:
            LOGGER.error('Error: Failed to get loss numbers')
            file_skeleton(OUTFILE)
            sys.exit()

        sequence = ['CatalogTypeCode', 'ModelCode', 'PortGuSD', 'CalculatedPortGuSD',
                    'DifferencePortGuSD_Percent', 'PortGrSD', 'CalculatedPortGrSD',
                    'DifferencePortGrSD_Percent', 'Status']
        resultDF_summary = set_column_sequence(resultDF_summary, sequence)

        resultDF_summary.to_csv(OUTFILE, index=False)
        resultDF_detailed.to_csv(OUTFILE[:-4] + '-Detailed.csv', index=False)

        LOGGER.info('----------------------------------------------------------------------------------')
        LOGGER.info('         Correlation Validation Completed Successfully                            ')
        LOGGER.info('----------------------------------------------------------------------------------')

        LOGGER.info('********** Process Complete Time: ' + str(time.time() - start) + ' Seconds **********')

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
        file_skeleton(OUTFILE)
        sys.exit()

