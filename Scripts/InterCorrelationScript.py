#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
******************************************************
Inter Correlation Validation Script
******************************************************

"""

# Import standard Python packages
import getopt
import sys
import time
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Import internal packages
from ValidationLib.general.CsvTools.main import _saveDFCsv
from ValidationLib.database.main import *
from ValidationLib.financials.Correlation.main import *


__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'


if __name__ == '__main__':

    start = time.time()
    # Extract the given arguments
    '''

    Input:

    1. Arg(3) - Server
    2. Arg(4) - Result DB
    3. Arg(5) - Result Path (As an Outfile)
    4. Arg(6) - Contract Analysis SID
    5. Arg(7) - Tolerance

    Output:

    1. Summary File ( To be considered as a base file)
    2. Detailed File

    '''
    server = sys.argv[3]
    result_Db = sys.argv[4]
    optlist, args = getopt.getopt(sys.argv[1:], [''], ['outfile='])

    outfile = None
    for o, a in optlist:
        if o == "--outfile":
            outfile = a
        print ("Outfile: " + outfile)
    if outfile is None:
        raise Exception("outfile not passed into script")
    contract_analysisSID = sys.argv[6]
    tolerance = sys.argv[7]

    handler_info = logging.FileHandler(outfile[:-4] + 'info.log')
    handler_info.setLevel(logging.INFO)

    logger.info('**********************************************************************************')
    logger.info('                     Correlation Validation Tool                                  ')
    logger.info('**********************************************************************************')

    # Initialize the connection with the server
    validation = dbConnection(server)
    corrValidation = CorrValidation(server)

    logger.info('**********************************************************************************************************')
    logger.info('Step 1. Getting the Intra and Inter Correlation Factors')
    # Extract the correlation factors using Contract Analtsis SID
    intraCorrelation, interCorrelation = corrValidation._get_correlation_factor(contract_analysisSID)
    logger.info('1. Intra Correlation Factor: ' + str(intraCorrelation))
    logger.info('2. Inter Correlation Factor: ' + str(interCorrelation))
    logger.info('**********************************************************************************************************')

    logger.info('**********************************************************************************************************')
    logger.info('Step 2. Getting the contract result SID')
    # Get the result SID using Location and Contract Analysis SID
    contractResultSID = validation._getResultSID(contract_analysisSID)
    logger.info('1. Contract Result SID: ' + str(contractResultSID))
    logger.info('**********************************************************************************************************')

    logger.info('**********************************************************************************************************')
    logger.info('Step 3. Getting the loss numbers and validating them')
    # Validate the correlation equation
    resultDF_detailed, resultDF_summary = corrValidation._get_sd(contractResultSID, result_Db, 'Inter',
                                                                 interCorrelation=intraCorrelation, tolerance=tolerance)
    logger.info('**********************************************************************************************************')

    logger.info('**********************************************************************************************************')
    logger.info('Ste 4. Saving the results')
    _saveDFCsv(resultDF_detailed, outfile[:-4] + '-Detailed.csv')
    _saveDFCsv(resultDF_summary, outfile)
    logger.info('**********************************************************************************************************')

    logger.info('----------------------------------------------------------------------------------')
    logger.info('                     Correlation Validation Completed                             ')
    logger.info('----------------------------------------------------------------------------------')

    logger.info('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')

