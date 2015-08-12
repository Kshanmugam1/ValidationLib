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
    # optlist, args = getopt.getopt(sys.argv[1:], [''], ['outfile='])
    #
    # outfile = None
    # for o, a in optlist:
    #     if o == "--outfile":
    #         outfile = a
    #     print ("Outfile: " + outfile)
    # if outfile is None:
    #     raise Exception("outfile not passed into script")
    outfile = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\EPValidation.csv'
    analysisSID = 939

    validation = dbConnection(server)

    print('**********************************************************************************************************')
    print('Step 1. Getting the result SID')
    # Get the result SID using Location and Contract Analysis SID
    resultSID = validation._getResultSID(analysisSID)
    print('1. Result SID: ' + str(resultSID))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 2. Getting GU Loss By Event')
    # Get the result SID using Location and Contract Analysis SID
    event_gu_occ_lossDf = validation._get_event_loss_ep(result_Db, resultSID, 'GU', 'Occ')
    event_gr_occ_lossDf = validation._get_event_loss_ep(result_Db, resultSID, 'GR', 'Occ')

    event_gu_agg_lossDf = validation._get_event_loss_ep(result_Db, resultSID, 'GU', 'Agg')
    event_gr_agg_lossDf = validation._get_event_loss_ep(result_Db, resultSID, 'GR', 'Agg')
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 2. Getting EP Los Annual Ep Table')
    # Get the result SID using Location and Contract Analysis SID
    ep_lossDf = validation._getLossDF(result_Db, resultSID, 'EP').iloc[:, :14]
    columns = ['EPAnnualTypeCode', 'EPCurveTypeCode', 'FinancialPerspectiveCode', 'Rank', 'ModelCode', 'YearID', 'PerilSetCode', 'ExceedanceProbability', 'EPLoss']
    ep_lossDf_gu_occ = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode']=='STD') & (ep_lossDf['FinancialPerspectiveCode']=='GU') & (ep_lossDf['EPAnnualTypeCode']=='OCC'), columns]
    ep_lossDf_gr_occ = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode']=='STD') & (ep_lossDf['FinancialPerspectiveCode']=='GR') & (ep_lossDf['EPAnnualTypeCode']=='OCC'), columns]

    ep_lossDf_gu_agg = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode']=='STD') & (ep_lossDf['FinancialPerspectiveCode']=='GU') & (ep_lossDf['EPAnnualTypeCode']=='AGG'), columns]
    ep_lossDf_gr_agg = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode']=='STD') & (ep_lossDf['FinancialPerspectiveCode']=='GR') & (ep_lossDf['EPAnnualTypeCode']=='AGG'), columns]

    result_lossdf_gu_occ = ep_lossDf_gu_occ
    result_lossdf_gu_occ['CalcLoss'] = event_gu_occ_lossDf['GU'].values
    result_lossdf_gu_occ['CalcModelCode'] = event_gu_occ_lossDf['ModelCode'].values
    result_lossdf_gu_occ['CalcYearID'] = event_gu_occ_lossDf['YearID'].values
    result_lossdf_gu_occ['CalcRank'] = event_gu_occ_lossDf.index.values + 1
    result_lossdf_gu_occ['CalcExceedanceProb'] = result_lossdf_gu_occ['CalcRank'] / 10000.0

    result_lossdf_gr_occ = ep_lossDf_gr_occ
    result_lossdf_gr_occ['CalcLoss'] = event_gr_occ_lossDf['GR'].values
    result_lossdf_gr_occ['CalcModelCode'] = event_gr_occ_lossDf['ModelCode'].values
    result_lossdf_gr_occ['CalcYearID'] = event_gr_occ_lossDf['YearID'].values
    result_lossdf_gr_occ['CalcRank'] = event_gr_occ_lossDf.index.values + 1
    result_lossdf_gr_occ['CalcExceedanceProb'] = result_lossdf_gr_occ['CalcRank'] / 10000.0

    result_lossdf_gu_agg = ep_lossDf_gu_agg
    result_lossdf_gu_agg['CalcLoss'] = event_gu_agg_lossDf['GU'].values
    result_lossdf_gu_agg['CalcModelCode'] = event_gu_agg_lossDf['ModelCode'].values
    result_lossdf_gu_agg['CalcYearID'] = event_gu_agg_lossDf['YearID'].values
    result_lossdf_gu_agg['CalcRank'] = event_gu_agg_lossDf.index.values + 1
    result_lossdf_gu_agg['CalcExceedanceProb'] = result_lossdf_gu_agg['CalcRank'] / 10000.0

    result_lossdf_gr_agg = ep_lossDf_gr_agg
    result_lossdf_gr_agg['CalcLoss'] = event_gr_agg_lossDf['GR'].values
    result_lossdf_gr_agg['CalcModelCode'] = event_gr_agg_lossDf['ModelCode'].values
    result_lossdf_gr_agg['CalcYearID'] = event_gr_agg_lossDf['YearID'].values
    result_lossdf_gr_agg['CalcRank'] = event_gr_agg_lossDf.index.values + 1
    result_lossdf_gr_agg['CalcExceedanceProb'] = result_lossdf_gr_agg['CalcRank'] / 10000.0

    result_occ = pd.concat([result_lossdf_gu_occ, result_lossdf_gr_occ], axis=0)
    result_agg = pd.concat([result_lossdf_gu_agg, result_lossdf_gr_agg], axis=0)
    pd.concat([result_occ, result_agg], axis=0).to_csv('sample.csv', index=False)

    columns = ['EPAnnualTypeCode', 'EPCurveTypeCode', 'EPTypeCode', 'FinancialPerspective', 'ExpectedValue', 'StandardDeviation']
    ep_points = validation._get_ep_points(analysisSID)
    for i in range(len(ep_points)):
        columns.append('EP' + str(i+1) + 'Loss')

    result_ep_summary = pd.DataFrame(columns=columns)
    annual_type_code = ['AGG', 'OCC']
    financial_perspective = ['GU', 'GR']

    count = 0
    for j in annual_type_code:
        for i in financial_perspective:
            result_ep_summary.loc[count, 'EPAnnualTypeCode'] = j
            result_ep_summary.loc[count, 'EPCurveTypeCode'] = 'STD'
            result_ep_summary.loc[count, 'EPTypeCode'] = 'EP'
            result_ep_summary.loc[count, 'FinancialPerspective'] = i
            count += 1

    for i in range(len(ep_points)):
        try:
            result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'EP' + str(i+1) + 'Loss'] = result_lossdf_gu_occ.loc[result_lossdf_gu_occ['CalcExceedanceProb'] == ep_points[i], 'CalcLoss'].values[0]
        except:
            result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'EP' + str(i+1) + 'Loss'] = 0.0
    result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'ExpectedValue'] = result_lossdf_gu_occ['CalcLoss'].sum()/10000.0
    result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'StandardDeviation'] = math.sqrt((((result_lossdf_gu_occ['CalcLoss'] - result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'ExpectedValue'].values[0]))**2).sum()/9999)

    for i in range(len(ep_points)):
        try:
            result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'EP' + str(i+1) + 'Loss'] = result_lossdf_gu_agg.loc[result_lossdf_gu_agg['CalcExceedanceProb'] == ep_points[i], 'CalcLoss'].values[0]
        except:
            result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'EP' + str(i+1) + 'Loss'] = 0.0
    result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'ExpectedValue'] = result_lossdf_gu_agg['CalcLoss'].sum()/10000.0
    result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'StandardDeviation'] = math.sqrt((((result_lossdf_gu_agg['CalcLoss'] - result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GU') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'ExpectedValue'].values[0]))**2).sum()/9999)

    for i in range(len(ep_points)):
        try:
            result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'EP' + str(i+1) + 'Loss'] = result_lossdf_gr_occ.loc[result_lossdf_gr_occ['CalcExceedanceProb'] == ep_points[i], 'CalcLoss'].values[0]
        except:
            result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'EP' + str(i+1) + 'Loss'] = 0.0
    result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'ExpectedValue'] = result_lossdf_gr_occ['CalcLoss'].sum()/10000.0
    result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'StandardDeviation'] = math.sqrt((((result_lossdf_gr_occ['CalcLoss'] - result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'OCC'), 'ExpectedValue'].values[0]))**2).sum()/9999)

    for i in range(len(ep_points)):
        try:
            result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'EP' + str(i+1) + 'Loss'] = result_lossdf_gr_agg.loc[result_lossdf_gr_agg['CalcExceedanceProb'] == ep_points[i], 'CalcLoss'].values[0]
        except:
            result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'EP' + str(i+1) + 'Loss'] = 0.0
    result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'ExpectedValue'] = result_lossdf_gr_agg['CalcLoss'].sum()/10000.0
    result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'StandardDeviation'] = math.sqrt((((result_lossdf_gr_occ['CalcLoss'] - result_ep_summary.loc[(result_ep_summary['FinancialPerspective'] == 'GR') & (result_ep_summary['EPAnnualTypeCode'] == 'AGG'), 'ExpectedValue'].values[0]))**2).sum()/9999)

    print(result_ep_summary)
    validation._get_ep_points(analysisSID)
    print('**********************************************************************************************************')
    print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')


