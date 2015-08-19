#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Quota Share Validation Script
~~~~~~~~~~~~~~~~~~~~~~~~
This script demonstrates the flow of logic to validate Quota Share Treaty.

:copyright: (c) 2015 by Shashank Kapadia for AIR Worldwide
"""

# Import standard Python packages
import time
import getopt
import sys
import logging
import multiprocessing as mp

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler_info = logging.FileHandler('QuotaShare_info.log')
handler_info.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# handler_info.setFormatter(formatter)
logger.addHandler(handler_info)

# Import internal packages
from ValidationLib.database.main import *
from ValidationLib.financials.QuotaShare.main import *
from ValidationLib.financials.QuotaShare.main import _getRecovery
from ValidationLib.general.CsvTools.main import _saveDFCsv

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

if __name__ == '__main__':

    '''

    Input:

    :param server: Server which was used to run analysis
    :param result_db: Database where the result was saved
    :param outfile: Path the save the output
    :param analysis_SID: Analysis SID

    Output:

    :return resultDF: Comparison between the given and calculated Post-CAT net loss
    :rtype: CSV file.

    '''
    start = time.time()
    # Extract the given arguments
    server = 'QAWUDB2\SQL2012'
    result_Db = 'SKQSRes'

    # optlist, args = getopt.getopt(sys.argv[1:], [''], ['outfile='])
    # outfile = None
    # for o, a in optlist:
    #     if o == "--outfile":
    #         outfile = a
    #     print ("Outfile: " + outfile)
    # if outfile is None:
    #     raise Exception("outfile not passed into script")
    outfile = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\Qs_validation.csv'
    analysis_SID = 862

    # Initialize the connection with the server
    validation = Database(server)
    Program = QSValidation(server)

    logger.info('*****************************************************************************************************')
    logger.info('Step 1. Getting result SID')
    resultSID = validation._getResultSID(analysis_SID)
    logger.info('Result SID: ' + str(resultSID))

    logger.info('*****************************************************************************************************')
    logger.info('Step 2. Getting Program ID')
    programSID = validation._getProgramID(analysis_SID)
    logger.info('Program ID: ' + str(programSID))

    logger.info('*****************************************************************************************************')
    logger.info('Step 3. Program Info')
    programInfo = validation._getProgramInfo(programSID, 'qs')
    programInfo = pd.DataFrame(data=zip(*programInfo), columns=['Occ_Limit', 'Agg_Limit',
                                                                '%Ceded', '%Placed', 'Inuring'])
    logger.info(programInfo)

    logger.info('*****************************************************************************************************')
    logger.info('Step 4. Getting the task list')
    tasks, lossDF = Program._GetTasks(result_Db, resultSID)

    max_inuring = max(programInfo['Inuring'].values)
    recovery = []
    perRiskRecovery = []
    for k in range(max_inuring):

        program_infos = programInfo.loc[programInfo['Inuring'] == k + 1, :].values
        for j in range(len(program_infos)):
            pool = mp.Pool()
            results = [pool.apply_async(_getRecovery, args=(tasks[i], lossDF, program_infos[j]))
                       for i in range(len(tasks))]
            output = [p.get() for p in results]

            recovery.append([item[1] for sublist in output for item in sublist])
            perRiskRecovery.append([item[0] for sublist in output for item in sublist])

        recovery = [sum(x) for x in zip(*recovery)]
        recovery = [min(x) for x in zip(lossDF['GrossLoss'], recovery)]
        lossDF['RecoveryAfterTerms'] = recovery
        lossDF['CalculatedPerRiskRecovery'] = perRiskRecovery[0]
        if k == max_inuring - 1:
            lossDF['CalculatedNetOfPreCATLoss'] = lossDF['GrossLoss'] - lossDF['RecoveryAfterTerms']
        else:
            lossDF['GrossLoss'] = lossDF['GrossLoss'] - lossDF['RecoveryAfterTerms']
        recovery = []
        perRiskRecovery = []

    # logger.info('*****************************************************************************************************')
    # logger.info('Step 6.Validating Result dF')
    # resultDF = Program.validate(lossDF)

    logger.info('*****************************************************************************************************')
    logger.info('Ste 7. Saving the results')
    _saveDFCsv(lossDF, outfile)

    logger.info('----------------------------------------------------------------------------------')
    logger.info('                     Quota Share Validation Completed                             ')
    logger.info('----------------------------------------------------------------------------------')

    logger.info('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')