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

optlist, args = getopt.getopt(sys.argv[1:], [''], ['outfile='])

outfile = None
for o, a in optlist:
    if o == "--outfile":
        outfile = a
    print ("Outfile: " + outfile)
if outfile is None:
    raise Exception("outfile not passed into script")

import time
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler_info = logging.FileHandler(outfile[:-4] + 'info.log')
handler_info.setLevel(logging.INFO)
logger.addHandler(handler_info)

# Import internal packages
from ValidationLib.general.CsvTools.main import _saveDFCsv
from ValidationLib.general.main import *
from ValidationLib.database.main import *
from ValidationLib.financials.Correlation.main import *


__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'

start = time.time()

# Extract the given arguments
server = sys.argv[3]
result_Db = sys.argv[4]
contract_analysisName = sys.argv[5]
tolerance = sys.argv[6]

logger.info('**********************************************************************************')
logger.info('                     Correlation Validation Tool                                  ')
logger.info('**********************************************************************************')

# Initialize the connection with the server
validation = dbConnection(server)
corrValidation = CorrValidation(server)
contract_analysisSID = validation._getAnaysisSID(contract_analysisName)

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
resultDF_detailed, resultDF_summary = corrValidation._get_sd(contractResultSID, result_Db, 'Inter', inter_correlation=interCorrelation, tolerance=tolerance)
logger.info('**********************************************************************************************************')

logger.info('**********************************************************************************************************')
logger.info('Ste 4. Saving the results')
sequence = ['CatalogTypeCode', 'ModelCode', 'PortGuSD', 'CalculatedPortGuSD', 'DifferencePortGuSD_Percent',
            'PortGrSD', 'CalculatedPortGrSD', 'DifferencePortGrSD_Percent', 'Status']
resultDF_summary = set_column_sequence(resultDF_summary, sequence)
_saveDFCsv(resultDF_detailed, outfile[:-4] + '-Detailed.csv')
_saveDFCsv(resultDF_summary, outfile)
logger.info('**********************************************************************************************************')

logger.info('----------------------------------------------------------------------------------')
logger.info('                     Correlation Validation Completed                             ')
logger.info('----------------------------------------------------------------------------------')

logger.info('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')

