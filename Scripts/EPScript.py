#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
EP Summary Validation Script (Prototype)

------ Needs Clarification and cleaning -------------
******************************************************

"""

# Import standard Python packages and read outfile
import getopt
import sys

sys.path.insert(0, r'\\qafile2\TS\Working Data\Shashank\Validation Library\ValidationLib')
import warnings

warnings.filterwarnings('ignore')

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print ("Outfile: " + OUTFILE)
# OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\EPCurve.csv'
if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging
import math
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
                          'EPTargetTypeCode', 'PerilSetCode', 'ModelCode', 'ExpectedValue', 'StandardDeviation',
                          'EPSum', 'CalcExpectedValue', 'CalcStandardDeviation',
                          'CalcPerilSetCode', 'CalcModelCode', 'CalcEPSum', 'Status']).to_csv(outfile, index=False)

# Extract the given arguments
try:
    server = sys.argv[3]
    result_db = sys.argv[4]
    analysis_name = sys.argv[5]
    option = sys.argv[6]
    tolerance = 10

    LOGGER.info('Server: ' + str(server))
    LOGGER.info('Result DB: ' + str(result_db))
    LOGGER.info('Analysis Name: ' + str(analysis_name))
    LOGGER.info('Option: ' + str(option))

except:
    LOGGER.error('Please verify the inputs')
    file_skeleton(OUTFILE)
    sys.exit()

if __name__ == '__main__':

    try:
        start = time.time()

        LOGGER.info('********************************')
        LOGGER.info('**      Touchstone v.3.01      **')
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
            if option == 'ViewByBoth':
                ep_lossDf = db.loss_df(result_db, result_sid, 'EP', option='PerilModel').iloc[:, :14]
            elif option == 'ViewByPeril':
                ep_lossDf = db.loss_df(result_db, result_sid, 'EP', option='Peril').iloc[:, :14]
            elif option == 'ViewByModel':
                ep_lossDf = db.loss_df(result_db, result_sid, 'EP', option='Model').iloc[:, :14]
            else:
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

        try:
            loss_by_event = pd.DataFrame()
            result_ep_detail = pd.DataFrame()
            result_ep_summary = pd.DataFrame()

            financial_perspective = ['GR', 'GU', 'NT', 'POST', 'RT']
            ep_type_code = ['AGG', 'OCC']
            ep_target_type = ['EVNT', 'REI']
            peril_codes = ep_lossDf.PerilSetCode.unique()
            model_codes = ep_lossDf.ModelCode.unique()

            columns_summary = ['CalcEPAnnualTypeCode', 'CalcEPCurveTypeCode', 'CalcEPTypeCode',
                               'CalcFinancialPerspectiveCode', 'CalcEPTargetTypeCode',
                               'CalcExpectedValue', 'CalcStandardDeviation']
            for k in range(len(ep_points)):
                columns_summary.append('CalcEP' + str(k+1) + 'Loss')

            #  Option = 'Peril'
            if option == 'ViewByPeril':
                for l in ep_target_type:
                    for i in ep_type_code:
                        for j in financial_perspective:
                            for peril in peril_codes:
                                if l == 'EVNT':
                                    try:
                                        loss_by_event = db.event_loss_ep(result_db, result_sid, j, i, value=peril, saveBy='Peril')
                                        loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                                        if len(loss_by_event) == 0:
                                            continue
                                    except:
                                        continue
                                else:
                                    if not (l == 'REI' and j in ['NT', 'POST', 'RT']):
                                        try:
                                            loss_by_event = db.rei_loss_ep(result_db, result_sid, j, i)
                                            loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                                            if len(loss_by_event) == 0:
                                                continue
                                        except:
                                            continue
                                    else:
                                        break
                                result_ep_df = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode'] == 'STD') &
                                                             (ep_lossDf['FinancialPerspectiveCode'] == j) &
                                                             (ep_lossDf['EPTargetTypeCode'] == l) &
                                                             (ep_lossDf['EPAnnualTypeCode'] == i) & (ep_lossDf['PerilSetCode'] == peril), columns_detail]
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
                                result_ep_summary_df.loc[0, 'CalcPerilSetCode'] = peril
                                for k in range(len(ep_points)):
                                    try:
                                        result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                                 (result_ep_summary_df['CalcEPAnnualTypeCode'] == i) & (result_ep_summary_df['CalcPerilSetCode'] == peril),
                                                                 'CalcEP' + str(k+1) + 'Loss'] = \
                                            result_ep_df.loc[result_ep_df['CalcExceedanceProb'] == ep_points[k], 'CalcLoss'].values[0]
                                    except:
                                        result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                                 (result_ep_summary_df['CalcEPAnnualTypeCode'] == i) & (result_ep_summary_df['CalcPerilSetCode'] == peril),
                                                                 'CalcEP' + str(k+1) + 'Loss'] = 0.0
                                result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                         (result_ep_summary_df['CalcEPAnnualTypeCode'] == i) & (result_ep_summary_df['CalcPerilSetCode'] == peril),
                                                         'CalcExpectedValue'] = result_ep_df['CalcLoss'].sum()/10000.0
                                result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                         (result_ep_summary_df['CalcEPAnnualTypeCode'] == i) & (result_ep_summary_df['CalcPerilSetCode'] == peril),
                                                         'CalcStandardDeviation'] = \
                                    math.sqrt(((result_ep_df['CalcLoss']**2).sum() -
                                               (result_ep_df['CalcLoss'].sum())**2/catalog_size)/(catalog_size - 1))
                                result_ep_summary = pd.concat([result_ep_summary, result_ep_summary_df], axis=0)
            #  Option = 'Model' or 'Peril/Model'
            if option == 'ViewByModel' or option == 'ViewByBoth':
                for l in ep_target_type:
                    for i in ep_type_code:
                        for j in financial_perspective:
                            for model in model_codes:
                                if l == 'EVNT':
                                    try:
                                        loss_by_event = db.event_loss_ep(result_db, result_sid, j, i, value=model, saveBy='Model')
                                        loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                                        if len(loss_by_event) == 0:
                                            continue
                                    except:
                                        continue
                                else:
                                    if not (l == 'REI' and j in ['NT', 'POST', 'RT']):
                                        try:
                                            loss_by_event = db.rei_loss_ep(result_db, result_sid, j, i)
                                            loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                                            if len(loss_by_event) == 0:
                                                continue
                                        except:
                                            continue
                                    else:
                                        break
                                result_ep_df = ep_lossDf.loc[(ep_lossDf['EPCurveTypeCode'] == 'STD') &
                                                             (ep_lossDf['FinancialPerspectiveCode'] == j) &
                                                             (ep_lossDf['EPTargetTypeCode'] == l) &
                                                             (ep_lossDf['EPAnnualTypeCode'] == i) & (ep_lossDf['ModelCode'] == model), columns_detail]
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
                                result_ep_summary_df.loc[0, 'CalcModelCode'] = model
                                for k in range(len(ep_points)):
                                    try:
                                        result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                                 (result_ep_summary_df['CalcEPAnnualTypeCode'] == i) & (result_ep_summary_df['CalcModelCode'] == model),
                                                                 'CalcEP' + str(k+1) + 'Loss'] = \
                                            result_ep_df.loc[result_ep_df['CalcExceedanceProb'] == ep_points[k], 'CalcLoss'].values[0]
                                    except:
                                        result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                                 (result_ep_summary_df['CalcEPAnnualTypeCode'] == i) & (result_ep_summary_df['CalcModelCode'] == model),
                                                                 'CalcEP' + str(k+1) + 'Loss'] = 0.0
                                result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                         (result_ep_summary_df['CalcEPAnnualTypeCode'] == i) & (result_ep_summary_df['CalcModelCode'] == model),
                                                         'CalcExpectedValue'] = result_ep_df['CalcLoss'].sum()/10000.0
                                result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                         (result_ep_summary_df['CalcEPAnnualTypeCode'] == i) & (result_ep_summary_df['CalcModelCode'] == model),
                                                         'CalcStandardDeviation'] = \
                                    math.sqrt(((result_ep_df['CalcLoss']**2).sum() -
                                               (result_ep_df['CalcLoss'].sum())**2/catalog_size)/(catalog_size - 1))
                                result_ep_summary = pd.concat([result_ep_summary, result_ep_summary_df], axis=0)
            #
            elif option == None or option == '' or option == 'ViewByNone':
                for l in ep_target_type:
                    for i in ep_type_code:
                        for j in financial_perspective:
                            if l == 'EVNT':
                                try:
                                    loss_by_event = db.event_loss_ep(result_db, result_sid, j, i)
                                    loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                                    if len(loss_by_event) == 0:
                                        continue
                                except:
                                    continue
                            else:
                                if not (l == 'REI' and j in ['NT', 'POST', 'RT']):
                                    try:
                                        loss_by_event = db.rei_loss_ep(result_db, result_sid, j, i)
                                        loss_by_event = loss_by_event.loc[loss_by_event[j] > 0, :]
                                        if len(loss_by_event) == 0:
                                            break
                                    except:
                                        continue
                                else:
                                    continue
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
                                    result_ep_summary_df.loc[
                                        (result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                        (result_ep_summary_df['CalcEPAnnualTypeCode'] == i),
                                        'CalcEP' + str(k + 1) + 'Loss'] = \
                                        result_ep_df.loc[
                                            result_ep_df['CalcExceedanceProb'] == ep_points[k], 'CalcLoss'].values[0]
                                except:
                                    result_ep_summary_df.loc[
                                        (result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                        (result_ep_summary_df['CalcEPAnnualTypeCode'] == i),
                                        'CalcEP' + str(k + 1) + 'Loss'] = 0.0
                            result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                     (result_ep_summary_df['CalcEPAnnualTypeCode'] == i),
                                                     'CalcExpectedValue'] = result_ep_df['CalcLoss'].sum() / 10000.0
                            result_ep_summary_df.loc[(result_ep_summary_df['CalcFinancialPerspectiveCode'] == j) &
                                                     (result_ep_summary_df['CalcEPAnnualTypeCode'] == i),
                                                     'CalcStandardDeviation'] = \
                                math.sqrt(((result_ep_df['CalcLoss'] ** 2).sum() -
                                           (result_ep_df['CalcLoss'].sum()) ** 2 / catalog_size) / (catalog_size - 1))
                            result_ep_summary = pd.concat([result_ep_summary, result_ep_summary_df], axis=0)

            ep_summary = db.ep_summary(result_db, result_sid, option)
            if option == 'ViewByPeril':
                temp = 'PERIL'
            elif option == 'ViewByModel' or option == 'ViewByBoth':
                temp = 'MODEL'
            else:
                temp = 'STD'
            try:
                ep_summary = ep_summary.loc[(ep_summary['EPCurveTypeCode'] == temp) &
                                            (ep_summary['EPTypeCode'] == 'EP') &
                                            (ep_summary['EPTargetTypeCode'].isin(ep_target_type)) &
                                            (ep_summary['FinancialPerspectiveCode'].isin(financial_perspective))].iloc[:,
                             [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]].reset_index()
            except:
                ep_summary = ep_summary.loc[(ep_summary['EPCurveTypeCode'] == temp) &
                                            (ep_summary['EPTypeCode'] == 'EP') &
                                            (ep_summary['EPTargetTypeCode'].isin(ep_target_type)) &
                                            (ep_summary['FinancialPerspectiveCode'].isin(financial_perspective))].iloc[:,
                             [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]].reset_index()

            ep_summary = ep_summary.drop('index', axis=1)
            if option == 'ViewByModel' or option == 'ViewByBoth':
                ep_summary = ep_summary.sort(['EPAnnualTypeCode', 'FinancialPerspectiveCode', 'ModelCode'])
                result_ep_summary['CalcPerilSetCode'] = ep_summary['PerilSetCode'].values
            else:
                ep_summary = ep_summary.sort(['EPAnnualTypeCode', 'FinancialPerspectiveCode'])

            ep_summary = ep_summary.reset_index(drop=True)

            result_ep_summary = result_ep_summary.sort(['CalcEPAnnualTypeCode', 'CalcEPCurveTypeCode',
                                                        'CalcEPTypeCode', 'CalcFinancialPerspectiveCode',
                                                        'CalcEPTargetTypeCode']).reset_index()
            result_ep_summary = result_ep_summary.drop('index', axis=1)
            result_ep_summary = set_column_sequence(result_ep_summary, columns_summary)

            for count in range(len(result_ep_summary.iloc[:, 1])):
                result_ep_summary.loc[count, 'CalcEPSum'] = result_ep_summary.iloc[count, 7:22].sum()
                if not option == '':
                    ep_summary.loc[count, 'EPSum'] = ep_summary.iloc[count, 11:26].sum()
                else:
                    ep_summary.loc[count, 'EPSum'] = ep_summary.iloc[count, 9:22].sum()

            result_ep_summary_output = result_ep_summary.drop(result_ep_summary.columns.values[7:22], axis=1)
            if not option == '':
                ep_summary_output = ep_summary.drop(ep_summary.columns.values[11:26], axis=1)
            else:
                ep_summary_output = ep_summary.drop(ep_summary.columns.values[9:22], axis=1)

            summary_output = pd.concat([ep_summary_output, result_ep_summary_output.iloc[:, 5:]], axis=1).fillna('-')
            detailed_output = pd.concat([ep_summary, result_ep_summary], axis=1).fillna('-')

        except:
            LOGGER.error('Error: Failed core algorithm')
            file_skeleton(OUTFILE)
            sys.exit()

        summary_output['Status'] = ''
        summary_output.loc[((summary_output['EPSum'] - summary_output['CalcEPSum']) <= tolerance/100.0) &
                           ((summary_output['ExpectedValue'] - summary_output['CalcExpectedValue']) <= tolerance/100.0) &
                           ((summary_output['StandardDeviation'] - summary_output['CalcStandardDeviation']) <= tolerance/100.0),
                           'Status'] = 'Pass'

        summary_output.loc[summary_output['Status'] == '', 'Status'] = 'Fail'

        summary_output.to_csv(OUTFILE, index=False)
        detailed_output.to_csv(OUTFILE[:-4] + '-Detailed.csv', index=False)

        LOGGER.info('----------------------------------------------------------------------------------')
        LOGGER.info('                  EP Validation Completed Successfully                            ')
        LOGGER.info('----------------------------------------------------------------------------------')

        LOGGER.info('********** Process Complete Time: ' + str(time.time() - start) + ' Seconds **********')

    except:
        LOGGER.error('Unknown error: Contact code maintainer: ' + __maintainer__)
        file_skeleton(OUTFILE)
        sys.exit()

