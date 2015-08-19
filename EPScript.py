__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

import sys
import getopt
from ValidationLib.database.main import *
import time
import math


if __name__ == '__main__':

    start = time.time()

    server = 'QAWUDB2\SQL2012'
    result_Db = 'SkResult'
    outfile = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\EPValidation.csv'
    analysisSID = 946
    type = 'debug'

    validation = Database(server)

    print('**********************************************************************************************************')
    print('Step 1. Getting the result SID')
    # Get the result SID using Analysis SID
    resultSID = validation._getResultSID(analysisSID)
    print('1. Result SID: ' + str(resultSID))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 2. Get Annual EP Loss')
    ep_lossDf = validation._getLossDF(result_Db, resultSID, 'EP').iloc[:, :14]
    columns_detail = ['EPAnnualTypeCode', 'EPCurveTypeCode', 'FinancialPerspectiveCodeCode', 'Rank',
               'ModelCode', 'YearID', 'PerilSetCode', 'ExceedanceProbability', 'EPLoss']
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 3. Determine Catalog Size')
    max_year = max(ep_lossDf['YearID'])
    if max_year <= 10000.0:
        catalog_size = 10000.0
    elif max_year <= 50000.0:
        catalog_size = 50000.0
    else:
        catalog_size = 100000.0
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 4. Get EP points')
    ep_points = validation._get_ep_points(analysisSID)
    print('**********************************************************************************************************')

    loss_by_event = pd.DataFrame()
    result_ep_detail = pd.DataFrame()
    result_ep_summary = pd.DataFrame()

    financial_perspective = ['GR', 'GU', 'NT', 'POST', 'RT']
    ep_type_code = ['AGG', 'OCC']
    ep_target_type = ['EVNT', 'REI']
    columns_summary = ['EPAnnualTypeCode', 'EPCurveTypeCode', 'EPTypeCode',
                       'FinancialPerspectiveCode', 'EPTargetTypeCode', 'ExpectedValue', 'StandardDeviation']
    for k in range(len(ep_points)):
        columns_summary.append('EP' + str(k+1) + 'Loss')

    print('**********************************************************************************************************')
    print('Step 4. Get Loss By Event for different Financial Persp')
    for l in ep_target_type:
        for i in ep_type_code:
            for j in financial_perspective:
                print(i,j,l)
                if l == 'EVNT':
                    loss_by_event = validation._get_event_loss_ep(result_Db, resultSID, j, i)
                    loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                else:
                    if not (l == 'REI' and j in ['NT', 'POST', 'RT']):
                        loss_by_event = validation._get_rei_loss_ep(result_Db, resultSID, j, i)
                    else:
                        break
                result_ep_df = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode'] == 'STD') &
                                             (ep_lossDf['FinancialPerspectiveCode'] == j) &
                                             (ep_lossDf['EPTargetTypeCode'] == l) &
                                             (ep_lossDf['EPAnnualTypeCode'] == i), columns_detail]
                result_ep_df['CalcLoss'] = loss_by_event[j].values
                result_ep_df['CalcModelCode'] = loss_by_event['ModelCode'].values
                result_ep_df['CalcYearID'] = loss_by_event['YearID'].values
                result_ep_df['CalcRank'] = loss_by_event.index.values + 1
                result_ep_df['CalcExceedanceProb'] = result_ep_df['CalcRank'] / catalog_size

                result_ep_detail = pd.concat([result_ep_detail, result_ep_df], axis=0)

                result_ep_summary_df = pd.DataFrame(columns=columns_summary)
                result_ep_summary_df.loc[0, 'EPAnnualTypeCode'] = i
                result_ep_summary_df.loc[0, 'EPCurveTypeCode'] = 'STD'
                result_ep_summary_df.loc[0, 'EPTypeCode'] = 'EP'
                result_ep_summary_df.loc[0, 'FinancialPerspectiveCode'] = j
                result_ep_summary_df.loc[0, 'EPTargetTypeCode'] = l
                for k in range(len(ep_points)):
                    try:
                        result_ep_summary_df.loc[(result_ep_summary_df['FinancialPerspectiveCode'] == j) &
                                              (result_ep_summary_df['EPAnnualTypeCode'] == i),
                                              'EP' + str(k+1) + 'Loss'] = \
                            result_ep_df.loc[result_ep_df['CalcExceedanceProb'] == ep_points[k], 'CalcLoss'].values[0]
                    except:
                        result_ep_summary_df.loc[(result_ep_summary_df['FinancialPerspectiveCode'] == j) &
                                              (result_ep_summary_df['EPAnnualTypeCode'] == i),
                                              'EP' + str(k+1) + 'Loss'] = 0.0
                result_ep_summary_df.loc[(result_ep_summary_df['FinancialPerspectiveCode'] == j) &
                                      (result_ep_summary_df['EPAnnualTypeCode'] == i), 'ExpectedValue'] = \
                    result_ep_df['CalcLoss'].sum()/10000.0
                result_ep_summary_df.loc[(result_ep_summary_df['FinancialPerspectiveCode'] == j) &
                                      (result_ep_summary_df['EPAnnualTypeCode'] == i), 'StandardDeviation'] = \
                    math.sqrt(((result_ep_df['CalcLoss']**2).sum() - (result_ep_df['CalcLoss'].sum())**2/10000.0)/9999.0)
                result_ep_summary = pd.concat([result_ep_summary, result_ep_summary_df], axis=0)
    result_ep_summary.to_csv('sample.csv')
    ep_summary = validation._get_ep_summary(result_Db, resultSID)
    ep_summary = ep_summary.loc[(ep_summary['EPCurveTypeCode'] == 'STD') &
                                (ep_summary['EPTypeCode'] == 'EP') &
                                (ep_summary['EPTargetTypeCode'].isin(ep_target_type)) &
                                (ep_summary['FinancialPerspectiveCode'].isin(financial_perspective))].iloc[:,
                 [0, 1, 2, 3, 4, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]].reset_index()
    ep_summary = ep_summary.drop('index', axis=1)
    ep_summary = ep_summary.sort(['EPAnnualTypeCode', 'FinancialPerspectiveCode'])

    result_ep_summary = result_ep_summary.sort(['EPAnnualTypeCode', 'EPCurveTypeCode',
                                                'EPTypeCode', 'FinancialPerspectiveCode', 'EPTargetTypeCode']).reset_index()
    result_ep_summary = result_ep_summary.drop('index', axis=1)
    summary_output = pd.concat(dict(Source=ep_summary, Calculated=result_ep_summary), axis=1).fillna('-')
    (summary_output.to_csv('Ouput.csv'))
    print('**********************************************************************************************************')
    print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')


