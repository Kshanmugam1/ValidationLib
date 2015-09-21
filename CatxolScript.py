#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
CATXOL Validation Script
******************************************************

"""
# Import standard Python packages and read outfile
import getopt
import sys
import datetime
import warnings

warnings.filterwarnings('ignore')

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print ("Outfile: " + OUTFILE)

if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging
import multiprocessing as mp

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HANDLER_INFO = logging.FileHandler(OUTFILE[:-4] + '-info.log')
HANDLER_INFO.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER_INFO)

# Import internal packages
from ValidationLib.financials.main import *
from ValidationLib.financials.main import recovery_catxol
from ValidationLib.general.main import *
from ValidationLib.database.main import *


__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'


def file_skeleton(outfile):

    pd.DataFrame(columns=['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID', 'NetOfPreCATLoss',
                          'Recovery', 'PostCATNetLoss', 'CalculatedPostCATNetLoss',
                          'DifferencePercent', 'Status']).to_csv(outfile, index=False)

# Extract the given arguments
try:
    server = sys.argv[3]
    result_db = sys.argv[4]
    analysis_name = sys.argv[5]
except:
    LOGGER.error('Please verify the inputs')
    file_skeleton(OUTFILE)
    sys.exit()

if __name__ == "__main__":

    try:
        start = time.time()

        LOGGER.info('********************************')
        LOGGER.info('**      Touchstone v.3.0      **')
        LOGGER.info('********************************')

        LOGGER.info('\n********** Log header **********\n')
        LOGGER.info('Description:   CATXOL Validation')
        LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
        LOGGER.info('Status:                Completed')

        LOGGER.info('\n********** Log Import Options **********\n')
        # Initialize the connection with the server
        try:
            db = Database(server)
            catxol = Catxol(server)
            LOGGER.info('Server: ' + str(server))
        except:
            LOGGER.error('Error: Check connection to database server')

        try:
            analysis_sid = db.analysis_sid(analysis_name)
            LOGGER.info('Analysis SID: ' + str(analysis_sid))
        except:
            LOGGER.error('Error: Failed to extract the analysis SID from analysis name')

        try:
            result_sid = db.result_sid(analysis_sid)
            LOGGER.info('Result SID: ' + str(result_sid))
        except:
            LOGGER.error('Error: Failed to extract the result SID from analysis SID')

        try:
            programSID = db.program_id(analysis_sid)
            LOGGER.info('Program ID: ' + str(programSID))
        except:
            LOGGER.error('Error: Failed to extract program ID')

        try:

            programInfo = db.program_info(programSID, 'catxol')
            programInfo = pd.DataFrame(data=zip(*programInfo), columns=['Occ_Limit', 'Occ_Ret', 'Agg_Limit',
                                                                        'Agg_Ret', '%Placed', 'Ins_CoIns',
                                                                        'Inuring'])
            LOGGER.info('Program Info: ')
            LOGGER.info(programInfo)
        except:
            LOGGER.error('Error: Failed to get program information')
        try:
            tasks, lossDF = catxol.get_tasks(result_db, result_sid)
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

                program_infos = programInfo.loc[programInfo['Inuring'] == k + 1, :].values

                for j in range(len(program_infos)):
                    pool = mp.Pool()
                    results = [pool.apply_async(recovery_catxol, args=(tasks[i], lossDF, program_infos[j]))
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
        except:
            LOGGER.error('Error: Failed multi-threading task')

        try:
            resultDF = catxol.validate(lossDF)
        except:
            LOGGER.error('Error: Failed to validate')

        sequence = ['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID', 'NetOfPreCATLoss', 'Recovery',
                    'PostCATNetLoss', 'CalculatedPostCATNetLoss', 'DifferencePercent', 'Status']
        resultDF = set_column_sequence(resultDF, sequence)

        resultDF.to_csv(OUTFILE, index=False)

        LOGGER.info('----------------------------------------------------------------------------------')
        LOGGER.info('              CATXOL Validation Completed Successfully                            ')
        LOGGER.info('----------------------------------------------------------------------------------')

        LOGGER.info('********** Process Complete Time: ' + str(time.time() - start) + ' Seconds **********')

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
        file_skeleton(OUTFILE)
        sys.exit()

