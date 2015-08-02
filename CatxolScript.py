# Import standard Python packages
import time
import multiprocessing as mp
import sys
import getopt
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler_info = logging.FileHandler('CATXOL_info.log')
handler_info.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler_info.setFormatter(formatter)
logger.addHandler(handler_info)

# Import internal packages
from ValidationLib.financials.Catxol.main import *
from ValidationLib.financials.Catxol.main import _getRecovery
from ValidationLib.general.CsvTools.main import _saveDFCsv

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'


if __name__ == '__main__':

    start = time.time()

    logger.info('**********************************************************************************')
    logger.info('                          CATXOL Validation Tool                                  ')
    logger.info('**********************************************************************************')
    # Extract the given arguments
    '''

    Input:

    1. Arg(3) - Server 'QAWUDB2\SQL2012'
    2. Arg(4) - Result DB 'SKCatRes'
    3. Arg(5) - Result Path (As an Outfile)
    4. Arg(6) - Analysis SID 765

    Output:

    1. Summary File

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

    analysis_SID = sys.argv[6]

    # Initialize the connection with the server
    validation = dbConnection(server)
    Program = ProgramValidation(server)

    logger.info('*****************************************************************************************************')
    logger.info('Step 1. Getting result SID')
    resultSID = validation._getResultSID(analysis_SID)
    logger.info('Result SID: ' + str(resultSID))

    logger.info('**********************************************************************************************************')
    logger.info('Step 2. Getting Program ID')
    programSID = validation._getProgramID(analysis_SID)

    logger.info('**********************************************************************************************************')
    logger.info('Step 3. Program Info')
    programInfo = validation._getProgramInfo(programSID)
    programInfo = pd.DataFrame(data=zip(*programInfo), columns=['Occ_Limit', 'Occ_Ret', 'Agg_Limit',
                                                                'Agg_Ret', '%Placed', 'Ins_CoIns', 'Inuring'])

    logger.info('**********************************************************************************************************')
    logger.info('Step 4. Getting the task list')
    tasks, lossDF = Program._GetTasks(result_Db, resultSID)

    logger.info('**********************************************************************************************************')
    logger.info('Step 5. Getting result DF')
    '''
    Pseudo Algorithm:
        1. Get the max inuring order
        2. For i=1 to max(inuring), i++
            get all the treaty at the inuring order i (Example: treat1 and treaty2 has a inuring number 1)
        3. Run each treaty at the same inuring order
    '''
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

    logger.info('**********************************************************************************************************')
    logger.info('Step 6.Validating Result dF')
    resultDF = Program._validate(lossDF)

    logger.info('**********************************************************************************')
    logger.info('Ste 7. Saving the results')
    _saveDFCsv(resultDF, outfile)

    logger.info('----------------------------------------------------------------------------------')
    logger.info('                          CATXOL Validation Completed                             ')
    logger.info('----------------------------------------------------------------------------------')

    logger.info('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')