# Import standard Python packages
import time
import multiprocessing as mp

# Import internal packages
from database.main import *
from financials.Catxol.main import *
from financials.Catxol.main import _getRecovery
from general.CsvTools.main import _saveDFCsv

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'

if __name__ == '__main__':

    start = time.time()

    print('**********************************************************************************')
    print('                          CATXOL Validation Tool                                  ')
    print('**********************************************************************************')
    # Extract the given arguments
    '''
    Input:

    1. Arg(3) - Server
    2. Arg(4) - Result DB
    3. Arg(5) - Result Path (As an Outfile)
    4. Arg(6) - Analysis SID

    Output:

    1. Summary File
    '''

    server = 'QAWUDB2\SQL2012'
    result_Db = 'SKCatRes'
    result_path =  r'C:\Users\i56228\Documents\Python\Git\ValidationLib\Catxol_Validation.csv'
    analysis_SID = 765

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
    programInfo = pd.DataFrame(data=zip(*programInfo), columns=['Occ_Limit', 'Occ_Ret', 'Agg_Limit',
                                                                'Agg_Ret', '%Placed', 'Ins_CoIns', 'Inuring'])

    print('**********************************************************************************************************')
    print('Step 4. Getting the task list')
    tasks, lossDF = Program._GetTasks(result_Db, resultSID)

    print('**********************************************************************************************************')
    print('Step 5. Getting result DF')
    max_inuring = max(programInfo['Inuring'].values)
    recovery = []
    for k in range(max_inuring):

        program_infos = programInfo.loc[programInfo['Inuring'] == k+1, :].values

        for j in range(len(program_infos)):
            pool = mp.Pool()
            results = [pool.apply_async(_getRecovery, args=(tasks[i], lossDF, program_infos[j]))
                       for i in range(len(tasks))]
            output = [p.get() for p in results]
            recovery.append([item for sublist in output for item in sublist])
        recovery = [sum(x) for x in zip(*recovery)]
        recovery = [min(x) for x in zip(lossDF['NetOfPreCATLoss'], recovery)]
        lossDF['Recovery'] = recovery
        if k == max_inuring - 1:
            lossDF['CalculatedPostCATNetLoss'] = lossDF['NetOfPreCATLoss'] - lossDF['Recovery']
        else:
            lossDF['NetOfPreCATLoss'] = lossDF['NetOfPreCATLoss'] - lossDF['Recovery']
        recovery = []

    print('**********************************************************************************************************')
    print('Step 6.Validating Result dF')
    resultDF = Program._validate(lossDF)

    print('**********************************************************************************')
    print('Ste 7. Saving the results')
    _saveDFCsv(resultDF, result_path)

    print('----------------------------------------------------------------------------------')
    print('                          CATXOL Validation Completed                             ')
    print('----------------------------------------------------------------------------------')

    print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')