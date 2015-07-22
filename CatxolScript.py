# Import internal packages
from DbConn.main import *
from Catxol.main import *
from CsvTools.main import _saveDFCsv

import time

if __name__ == '__main__':

    print('**********************************************************************************')
    print('                          CATXOL Validation Tool                                  ')
    print('**********************************************************************************')
    # Extract the given arguments
    '''
    1. Arg(3) - Server
    2. Arg(4) - Result DB
    3. Arg(5) - Result Path (As an Outfile)
    4. Arg(6) - Analysis SID
    '''

    server = 'QAWUDB2\SQL2012'
    result_Db = 'SKCatRes'
    result_path =  r'C:\Users\i56228\Documents\Python\Git\ValidationLib\Catxol_Validation.csv'
    analysis_SID = 635

    # Initialize the connection with the server
    validation = dbConnection(server)
    Program = ProgramValidation(server)

    print('**********************************************************************************************************')
    print('Step 1. Getting result SID')
    resultSID = validation._getResultSID(analysis_SID)

    print('**********************************************************************************************************')
    print('Step 2. Getting Program ID')
    programSID = validation._getProgramID(analysis_SID)

    print('**********************************************************************************************************')
    print('Step 3. Program Info')
    occ_limit, occ_ret, agg_limit, agg_ret, placed_percent, ins_coins = validation._getProgramInfo(programSID)
    print(occ_limit, occ_ret, agg_limit, agg_ret, placed_percent, ins_coins)
    print('**********************************************************************************************************')
    print('Step 5. Validate the Program')
    start = time.time()
    resultDF = Program._validate(result_Db, resultSID, occ_limit, occ_ret, agg_limit,
                                 agg_ret, placed_percent, ins_coins, 10)
    print('Finish Time: ' + str(float(time.time() - start)))

    print('**********************************************************************************')
    print('Ste 6. Saving the results')
    _saveDFCsv(resultDF, result_path)