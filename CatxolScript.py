# Import internal packages
from DbConn.main import *
from Catxol.main import *
from Catxol.main import _validate
from CsvTools.main import _saveDFCsv

import time
import multiprocessing as mp

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
    analysis_SID = 625

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
    programInfo = validation._getProgramInfo(programSID)
    print('**********************************************************************************************************')
    print('Step 4. Getting the task list')
    start = time.time()
    tasks, lossDF = Program._GetTasks(result_Db, resultSID)

    print('**********************************************************************************************************')
    print('Step 5. Getting result DF')
    pool = mp.Pool()
    results = [pool.apply_async(_validate, args=(tasks[i], lossDF, programInfo)) for i in range(len(tasks))]
    output = [p.get() for p in results]
    resultDF = Program._getResultDF(output)

    print('**********************************************************************************')
    print('Ste 6. Saving the results')
    _saveDFCsv(resultDF, result_path)

    print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')