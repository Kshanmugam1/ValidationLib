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

# Import standard Python packages and read outfile
import time
import logging

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HANDLER_INF0 = logging.FileHandler(OUTFILE[:-4] + '-info.log')
HANDLER_INF0.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER_INF0)

if OUTFILE is None:
    LOGGER.error('Outfile is not passed')
    sys.exit()

# Import internal packages
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

def file_skeleton(outfile):

    pd.DataFrame(columns=['CatalogTypeCode', 'ModelCode', 'PortGuSD', 'CalculatedPortGuSD',
                          'DifferencePortGuSD_Percent', 'PortGrSD', 'CalculatedPortGrSD',
                          'DifferencePortGrSD_Percent', 'Status']).to_csv(outfile, index=False)


start = time.time()

print('**********************************************************************************')
print('                     Correlation Validation Tool                                  ')
print('**********************************************************************************')
# Extract the given arguments
'''

Input:

1. Arg(3) - Server
2. Arg(4) - Result DB
3. Arg(5) - Result Path (As an Outfile)
4. Arg(6) - Contract Analysis SID
5. Arg(7) - Location AnalysisSID
6. Arg(8) - Tolerance

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
location_analysisSID = sys.argv[7]
tolerance = sys.argv[8]

# Initialize the connection with the server
validation = Database(server)
corrValidation = Correlation(server)

print('**********************************************************************************')
print('Step 1. Getting the Intra and Inter Correlation Factors')
# Extract the correlation factors using Contract Analtsis SID
intraCorrelation, interCorrelation = corrValidation._get_correlation_factor(contract_analysisSID)
print('1. Intra Correlation Factor: ' + str(intraCorrelation))
print('2. Inter Correlation Factor: ' + str(interCorrelation))
print('**********************************************************************************')

print('**********************************************************************************')
print('Step 2. Getting the contract and the location result SID')
# Get the result SID using Location and Contract Analysis SID
contractResultSID = validation._getResultSID(contract_analysisSID)
locationResultSID = validation._getResultSID(location_analysisSID)
print('1. Contract Result SID: ' + str(contractResultSID))
print('2. Location Result SID: ' + str(locationResultSID))
print('**********************************************************************************')

print('**********************************************************************************')
print('Step 3. Getting the loss numbers and validating them')
# Validate the correlation equation
resultDF_detailed, resultDF_summary = corrValidation._get_sd(contractResultSID, result_Db, 'Intra',
                                                             location_result_sid=locationResultSID,
                                                             intra_correlation=intraCorrelation, tolerance=tolerance)
print('**********************************************************************************')

print('**********************************************************************************')
print('Ste 4. Saving the results')
_saveDFCsv(resultDF_detailed, outfile[:-4] + '-Detailed.csv')
_saveDFCsv(resultDF_summary, outfile)
print('**********************************************************************************')

print('----------------------------------------------------------------------------------')
print('                     Correlation Validation Completed                             ')
print('----------------------------------------------------------------------------------')

print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')
