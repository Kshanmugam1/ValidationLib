#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
EP Summary Validation Script
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
# OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\EPValidation.csv'
if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging
import math

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

    pd.DataFrame(columns=['CatalogTypeCode', 'ModelCode', 'PortGuSD', 'CalculatedPortGuSD',
                          'DifferencePortGuSD_Percent', 'PortGrSD', 'CalculatedPortGrSD',
                          'DifferencePortGrSD_Percent', 'Status']).to_csv(outfile, index=False)

# Extract the given arguments
try:
    server = 'QAWUDB2\SQL2012'
    result_db = 'SkResult'
    analysis_name = 'Can_allLOB - Loss Analysis-Contract'
except:
    LOGGER.error('Please verify the inputs')
    file_skeleton(OUTFILE)
    sys.exit()

if __name__ == '__main__':

    start = time.time()

    LOGGER.info('**********************************************************************************')
    LOGGER.info('                      EP Summary Validation Tool                                  ')
    LOGGER.info('**********************************************************************************')

    # Initialize the connection with the server
    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 1. Establishing the connection with database and initialize the Correlation class')
    try:
        db = Database(server)
    except:
        LOGGER.error('Invalid server information')
        file_skeleton(OUTFILE)
        sys.exit()
    LOGGER.info('Connection Established with server: ' + str(server))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 2. Get analysis sid')
    analysis_sid = db.analysis_sid(analysis_name)
    LOGGER.info('Analysis SID for analysis ' + str(analysis_name) + ' is ' + str(analysis_sid))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 3. Get result SID')
    result_sid = db.result_sid(analysis_sid)
    LOGGER.info('Result SID: ' + str(result_sid))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 4. Get EP loss')
    ep_lossDf = db.loss_df(result_db, result_sid, 'EP').iloc[:, :14]
    columns_detail = ['EPAnnualTypeCode', 'EPCurveTypeCode', 'FinancialPerspectiveCodeCode', 'Rank',
                      'ModelCode', 'YearID', 'PerilSetCode', 'ExceedanceProbability', 'EPLoss']

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 5. Determine Catalog size')
    max_year = max(ep_lossDf['YearID'])
    if max_year <= 10000.0:
        catalog_size = 10000.0
    elif max_year <= 50000.0:
        catalog_size = 50000.0
    else:
        catalog_size = 100000.0
    LOGGER.info('Catalog Size: ' + str(catalog_size))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 6. Get EP points')
    ep_points = db.ep_points(analysis_sid)
    LOGGER.info('EP points: ' + str(ep_points))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 7. Get loss and validate')
    loss_by_event = pd.DataFrame()
    result_ep_detail = pd.DataFrame()
    result_ep_summary = pd.DataFrame()

    financial_perspective = ['GR', 'GU', 'NT', 'POST', 'RT']
    ep_type_code = ['AGG', 'OCC']
    ep_target_type = ['EVNT', 'REI']
    columns_summary = ['CalcEPAnnualTypeCode', 'CalcEPCurveTypeCode', 'CalcEPTypeCode',
                       'CalcFinancialPerspectiveCode', 'CalcEPTargetTypeCode',
                       'CalcExpectedValue', 'CalcStandardDeviation']
    for k in range(len(ep_points)):
        columns_summary.append('CalcEP' + str(k+1) + 'Loss')

    for l in ep_target_type:
        for i in ep_type_code:
            for j in financial_perspective:
                if l == 'EVNT':
                    try:
                        loss_by_event = db.event_loss_ep(result_db, result_sid, j, i)
                        loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                    except:
                        break
                else:
                    if not (l == 'REI' and j in ['NT', 'POST', 'RT']):
                        try:
                            loss_by_event = db.rei_loss_ep(result_db, result_sid, j, i)
                        except:
                            break
                    else:
                        break
                result_ep_df = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode'] == 'STD') &
                                             (ep_lossDf['FinancialPerspectiveCode'] == j) &
                                             (ep_lossDf['EPTargetTypeCode'] == l) &
                                             (ep_lossDf['EPAnnualTypeCode'] == i), columns_detail]

                result_ep_df['CalcLoss'] = loss_by_event[j].values
                result_ep_df['CalcYearID'] = loss_by_event['YearID'].values
                result_ep_df['CalcRank'] = loss_by_event.index.values + 1
                result_ep_df['CalcExceedanceProb'] = result_ep_df['CalcRank'] / catalog_size

                result_ep_detail = pd.concat([result_ep_detail, result_ep_df], axis=0)

                result_ep_summary_df = pd.DataFrame(columns=columns_summary)
                result_ep_summary_df.loc[0, 'CalcEPAnnualTypeCode'] = i
                result_ep_summary_df.loc[0, 'CalcEPCurveTypeCode'] = 'STD'
                result_ep_summary_df.loc[0, 'CalcEPTypeCode'] = 'EP'
                result_ep_summary_df.loc[0, 'CalcFinancialPerspectiveCode'] = j
                result_ep_summary_df.loc[0, 'CalcEPTargetTypeCode'] = l
                for k in range(len(ep_points)):
                    try:
                        result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                              (result_ep_summary_df['CalcEPAnnualTypeCode'] == i),
                                              'CalcEP' + str(k+1) + 'Loss'] = \
                            result_ep_df.loc[result_ep_df['CalcExceedanceProb'] == ep_points[k], 'CalcLoss'].values[0]
                    except:
                        result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                              (result_ep_summary_df['CalcEPAnnualTypeCode'] == i),
                                              'CalcEP' + str(k+1) + 'Loss'] = 0.0
                result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                      (result_ep_summary_df['CalcEPAnnualTypeCode'] == i),
                                         'CalcExpectedValue'] = result_ep_df['CalcLoss'].sum()/10000.0
                result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                      (result_ep_summary_df['CalcEPAnnualTypeCode'] == i),
                                         'CalcStandardDeviation'] = \
                    math.sqrt(((result_ep_df['CalcLoss']**2).sum() -
                               (result_ep_df['CalcLoss'].sum())**2/catalog_size)/(catalog_size - 1))
                result_ep_summary = pd.concat([result_ep_summary, result_ep_summary_df], axis=0)

    ep_summary = db.ep_summary(result_db, result_sid)
    ep_summary = ep_summary.loc[(ep_summary['EPCurveTypeCode'] == 'STD') &
                                (ep_summary['EPTypeCode'] == 'EP') &
                                (ep_summary['EPTargetTypeCode'].isin(ep_target_type)) &
                                (ep_summary['FinancialPerspectiveCode'].isin(financial_perspective))].iloc[:,
                 [0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]].reset_index()
    ep_summary = ep_summary.drop('index', axis=1)
    ep_summary = ep_summary.sort(['EPAnnualTypeCode', 'FinancialPerspectiveCode'])

    result_ep_summary = result_ep_summary.sort(['CalcEPAnnualTypeCode', 'CalcEPCurveTypeCode',
                                                'CalcEPTypeCode', 'CalcFinancialPerspectiveCode',
                                                'CalcEPTargetTypeCode']).reset_index()
    result_ep_summary = result_ep_summary.drop('index', axis=1)
    result_ep_summary = set_column_sequence(result_ep_summary, columns_summary)

    for count in range(len(result_ep_summary)):
        result_ep_summary.loc[count, 'CalcEPSum'] = result_ep_summary.iloc[count, 7:22].sum()
        ep_summary.loc[count, 'EPSum'] = ep_summary.iloc[count, 7:22].sum()

    result_ep_summary_output = result_ep_summary.drop(result_ep_summary.columns.values[7:22], axis=1)
    ep_summary_output = ep_summary.drop(ep_summary.columns.values[7:22], axis=1)

    summary_output = pd.concat([ep_summary_output, result_ep_summary_output.iloc[:, 5:]], axis=1).fillna('-')
    detailed_output = pd.concat([ep_summary, result_ep_summary], axis=1).fillna('-')

    summary_output['Status'] = -1
    summary_output.loc[(summary_output['EPSum'] == summary_output['CalcEPSum']) &
                       (summary_output['ExpectedValue'] == summary_output['CalcExpectedValue']) &
                       (summary_output['StandardDeviation'] == summary_output['CalcStandardDeviation']),
                       'Status'] = 'Pass'
    summary_output.loc[summary_output['Status'] == -1, 'Status'] = 'Fail'

    summary_output.to_csv(OUTFILE, index=False)
    detailed_output.to_csv(OUTFILE[:-4] + '-Detailed.csv', index=False)

    LOGGER.info('----------------------------------------------------------------------------------')
    LOGGER.info('                  EP Validation Completed Successfully                            ')
    LOGGER.info('----------------------------------------------------------------------------------')

    LOGGER.info('********** Process Complete Time: ' + str(time.time() - start) + ' Seconds **********')


