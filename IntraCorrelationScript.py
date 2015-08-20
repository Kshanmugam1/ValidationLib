#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Intra Correlation Validation Script
******************************************************

"""

# Import standard Python packages and read outfile
import getopt
import sys

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print "Outfile: " + OUTFILE

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

    pd.DataFrame(columns=['CatalogTypeCode', 'ModelCode', 'PerilSetCode', 'ContractID',
                          'ContractGuSD', 'CalculatedConGuSD',
                          'DifferenceConGuSD_Percent', 'ContractGrSD',
                          'CalculatedConGrSD', 'DifferenceConGrSD_Percent',
                          'Status']).to_csv(outfile, index=False)

start = time.time()

# Extract the given arguments
try:
    server = sys.argv[3]
    result_db = sys.argv[4]
    contract_analysis_name = sys.argv[5]
    location_analysis_name = sys.argv[6]
    tolerance = sys.argv[7]
except:
    LOGGER.error('Please verify the inputs')
    file_skeleton(OUTFILE)
    sys.exit()

if __name__ == "__main__":

    LOGGER.info('**********************************************************************************')
    LOGGER.info('                     Correlation Validation Tool                                  ')
    LOGGER.info('**********************************************************************************')

    # Initialize the connection with the server
    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 1. Establishing the connection with database and initialize the Correlation class')
    try:
        db = Database(server)
        intra_correlation = Correlation(server)
    except:
        LOGGER.error('Invalid server information')
        file_skeleton(OUTFILE)
        sys.exit()
    LOGGER.info('Connection Established with server: ' + str(server))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 2. Get analysis sid')
    contract_analysis_sid = db.analysis_sid(contract_analysis_name)
    location_analysis_sid = db.analysis_sid(location_analysis_name)
    LOGGER.info('Analysis SID for Contract analysis ' +
                str(contract_analysis_name) + ' is ' + str(contract_analysis_sid))
    LOGGER.info('Analysis SID for Location analysis ' +
                str(location_analysis_name) + ' is ' + str(location_analysis_sid))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 3. Get inter and intra correlation factor')
    intra_correlation_fac, inter_correlation_fac = intra_correlation.correlation_factor(contract_analysis_sid)
    LOGGER.info('1. Intra Correlation Factor: ' + str(intra_correlation_fac))
    LOGGER.info('2. Inter Correlation Factor: ' + str(inter_correlation_fac))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 4. Get result SID')
    contract_result_sid = db.result_sid(contract_analysis_sid)
    location_result_sid = db.result_sid(location_analysis_sid)
    LOGGER.info('Contract Result SID: ' + str(contract_result_sid))
    LOGGER.info('Location Result SID: ' + str(location_result_sid))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 5. Get the numbers and validate')
    resultDF_detailed, resultDF_summary = intra_correlation.loss_sd(contract_result_sid, result_db, 'Intra',
                                                                    location_result_sid=location_result_sid,
                                                                    intra_correlation=intra_correlation_fac,
                                                                    tolerance=tolerance)
    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 6. Save the results')
    sequence = ['CatalogTypeCode', 'ModelCode', 'PerilSetCode', 'ContractID', 'ContractGuSD', 'CalculatedConGuSD',
                'DifferenceConGuSD_Percent', 'ContractGrSD', 'CalculatedConGrSD', 'DifferenceConGrSD_Percent',
                'Status']
    resultDF_summary = set_column_sequence(resultDF_summary, sequence)

    resultDF_summary.to_csv(OUTFILE, index=False)
    resultDF_detailed.to_csv(OUTFILE[:-4] + '-Detailed.csv', index=False)

    LOGGER.info('----------------------------------------------------------------------------------')
    LOGGER.info('         Correlation Validation Completed Successfully                            ')
    LOGGER.info('----------------------------------------------------------------------------------')

    LOGGER.info('********** Process Complete Time: ' + str(time.time() - start) + ' Seconds **********')

