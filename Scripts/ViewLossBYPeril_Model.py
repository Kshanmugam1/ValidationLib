#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
EP Summary Validation Script: Peril & Model
******************************************************

"""

# Import standard Python packages and read outfile
import getopt
import sys

# sys.path.insert(0, r'\\qafile2\TS\Working Data\Shashank\Validation Library\ValidationLib')
import warnings

warnings.filterwarnings('ignore')

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print ("Outfile: " + OUTFILE)

OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\AnalysisOption.csv'

if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging
import datetime

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HANDLER_INF0 = logging.FileHandler(OUTFILE[:-4] + '-info.log')
HANDLER_INF0.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER_INF0)

# Import internal packages
from ValidationLib.database.main import *
from ValidationLib.general.main import *

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10 |Anaconda 2.3.0 (64-bit)'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'


def file_skeleton(outfile):
    pd.DataFrame(columns=['EPAnnualTypeCode', 'EPCurveTypeCode', 'EPTypeCode', 'FinancialPerspectiveCode',
                          'EPTargetTypeCode', 'ExpectedValue', 'StandardDeviation',
                          'EPSum', 'CalcExpectedValue', 'CalcStandardDeviation',
                          'Status']).to_csv(outfile, index=False)


# Extract the given arguments
try:
    server = 'QA-TS-CI4-DB\SQL2012'
    result_db = 'SKRes'
    analysis_name = 'SKExp - Loss Analysis_ByPeril'

except:
    LOGGER.error('Please verify the inputs')
    file_skeleton(OUTFILE)
    sys.exit()

if __name__ == '__main__':

    # try:
    start = time.time()

    LOGGER.info('********************************')
    LOGGER.info('**      Touchstone v.3.0      **')
    LOGGER.info('********************************')

    LOGGER.info('\n********** Log header **********\n')
    LOGGER.info('Description:   EP Summary Validation')
    LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
    LOGGER.info('Status:                Completed')

    LOGGER.info('\n********** Log Import Options **********\n')
    # Initialize the connection with the server
    try:
        db = Database(server)
        LOGGER.info('Server: ' + str(server))
    except:
        LOGGER.error('Error: Check connection to database server')
        file_skeleton(OUTFILE)
        sys.exit()

    try:
        analysis_sid = db.analysis_sid(analysis_name)
        LOGGER.info('Analysis SID: ' + str(analysis_sid))
    except:
        LOGGER.error('Error: Failed to extract the analysis SID from analysis name')
        file_skeleton(OUTFILE)
        sys.exit()

    try:
        result_sid = db.result_sid(analysis_sid)
        LOGGER.info('Result SID: ' + str(result_sid))
    except:
        LOGGER.error('Error: Failed to extract the result SID from analysis SID')
        file_skeleton(OUTFILE)
        sys.exit()

    try:
        analysis_information = db.analysis_option(analysis_sid)
    except:
        LOGGER.error('Error: Unable to fetch analysis option details')
        file_skeleton(OUTFILE)
        sys.exit()

    try:
        ep_lossDf = db.loss_df(result_db, result_sid, 'EP').iloc[:, :14]
    except:
        LOGGER.error('Error: Failed to fetch the loss DF')
        file_skeleton(OUTFILE)
        sys.exit()

    columns_detail = ['EPAnnualTypeCode', 'EPCurveTypeCode', 'FinancialPerspectiveCode', 'Rank',
                      'ModelCode', 'YearID', 'PerilSetCode', 'ExceedanceProbability', 'EPLoss']

    max_year = max(ep_lossDf['YearID'])
    if max_year <= 10000.0:
        catalog_size = 10000.0
    elif max_year <= 50000.0:
        catalog_size = 50000.0
    else:
        catalog_size = 100000.0
    LOGGER.info('Catalog Size: ' + str(catalog_size))

    try:
        ep_points = db.ep_points(analysis_sid)
        LOGGER.info('EP points: ' + str(ep_points))
    except:
        LOGGER.error('Error: Failed to get EP points')
        file_skeleton(OUTFILE)
        sys.exit()

    loss_by_event = pd.DataFrame()
    result_ep_detail = pd.DataFrame()
    result_ep_summary = pd.DataFrame()

    financial_perspective = ['GR', 'GU', 'NT', 'POST', 'RT']
    ep_type_code = ['AGG', 'OCC']
    ep_target_type = ['EVNT', 'REI']

    # if analysis_information['SaveSummaryByPeril'][0] and analysis_information['SaveSummaryByModel'][0]:
    #     LOGGER.info('Peril + Model currently not supported')
    if analysis_information['SaveSummaryByPeril'][0]:
        peril_codes = ep_lossDf.PerilSetCode.unique()
        columns_summary = ['CalcEPAnnualTypeCode', 'CalcEPCurveTypeCode', 'CalcEPTypeCode',
                           'CalcFinancialPerspectiveCode', 'PerilCode', 'CalcEPTargetTypeCode',
                           'CalcExpectedValue', 'CalcStandardDeviation']
        for k in range(len(ep_points)):
            columns_summary.append('CalcEP' + str(k + 1) + 'Loss')
        # try:
        for l in ep_target_type:
            for i in ep_type_code:
                for j in financial_perspective:
                    for m in peril_codes:
                        if l == 'EVNT':
                            try:
                                loss_by_event = db.event_loss_ep(result_db, result_sid, j, i, m, 'Peril')
                                loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                                if len(loss_by_event) == 0:
                                    break
                            except:
                                break
                        else:
                            if not (l == 'REI' and j in ['NT', 'POST', 'RT']):
                                try:
                                    loss_by_event = db.rei_loss_ep(result_db, result_sid, j, i)
                                    loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                                    if len(loss_by_event) == 0:
                                        break
                                except:
                                    break
                            else:
                                break
                        try:
                            result_ep_df = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode'] == 'STD') &
                                                         (ep_lossDf['FinancialPerspectiveCode'] == j) &
                                                         (ep_lossDf['EPTargetTypeCode'] == l) &
                                                         (ep_lossDf['EPAnnualTypeCode'] == i) & (
                                                             ep_lossDf['PerilSetCode'] == m), columns_detail]
                            loss_by_event.to_csv('tLossByevent.csv')
                            result_ep_df.to_csv('tLossAnnualEP.csv')
                            result_ep_df['CalcLoss'] = loss_by_event[j].values
                            result_ep_df['CalcYearID'] = loss_by_event['YearID'].values
                            result_ep_df['CalcRank'] = loss_by_event.index.values + 1
                            result_ep_df['CalcExceedanceProb'] = result_ep_df['CalcRank'] / catalog_size

                            result_ep_detail = pd.concat([result_ep_detail, result_ep_df], axis=0)
                            print result_ep_detail
                        except:
                            break




                            # try:
                            #     ep_lossDf = db.loss_df(result_db, result_sid, 'EP').iloc[:, :14]
                            # except:
                            #     LOGGER.error('Error: Failed to fetch the loss DF')
                            #     file_skeleton(OUTFILE)
                            #     sys.exit()
                            # except:
                            #     LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
                            #     file_skeleton(OUTFILE)
                            #     sys.exit()
