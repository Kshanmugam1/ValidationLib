__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'

# Import internal packages
from DbConn.main import *
from Correlation.main import *
from CsvTools.main import _saveDFCsv


if __name__ == '__main__':

    print('**********************************************************************************')
    print('                     Correlation Validation Tool                                  ')
    print('**********************************************************************************')
    # Extract the given arguments
    '''
    1. Arg(1) - Server
    2. Arg(2) - Result DB
    3. Arg(3) - REsult Path
    4. Arg(4) - Contract Analysis SID
    5. Arg(5) - Location AnalysisSID
    '''
    server = 'QAWUDB2\SQL2012' #'QAWUDB2\SQL2012'  # QA-TS-TPZ-DB1\SQL2014 #sys.argv[1]
    result_Db = 'SKResCorr' #sys.argv[2]
    result_path = r'C:\Users\i56228\Documents\Python\Git\ValidationLib' #sys.argv[3]
    contract_analysisSID = 424 #sys.argv[4]
    location_analysisSID = 399 #sys.argv[5]

    # Initialize the connection with the server
    validation = dbConnection(server)
    corrValidation = CorrValidation(server)

    print('**********************************************************************************')
    print('Step 1. Getting the Intra and Inter Correlation Factors')
    # Extract the correlation factors using Contract Analtsis SID
    intraCorrelation, interCorrelation = corrValidation._getCorrelationFactor(contract_analysisSID)
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
    resultDF_detailed, resultDF_summary = corrValidation._getSD(contractResultSID, result_Db, 'Intra',
                                                                locationResultSID=locationResultSID,
                                                                intraCorrelation=intraCorrelation)
    print('**********************************************************************************')

    print('**********************************************************************************')
    print('Ste 4. Saving the results')
    _saveDFCsv(resultDF_detailed, result_path, 'Detailed-Intra')
    _saveDFCsv(resultDF_summary, result_path, 'Summary-Intra')
    print('**********************************************************************************')

    print('----------------------------------------------------------------------------------')
    print('                     Correlation Validation Completed                             ')
    print('----------------------------------------------------------------------------------')