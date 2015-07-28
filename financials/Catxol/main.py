__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# Import internal packages
from database.main import *

# Import external Python libraries
import pandas as pd

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]

def _getRecovery(tuple, lossDF, programInfo):

    agg_limit_temp = copy.deepcopy(programInfo[2])
    agg_ret_temp = copy.deepcopy(programInfo[3])
    sample_lossDF = lossDF.loc[(lossDF['CatalogTypeCode'] == tuple[0]) & (lossDF['ModelCode'] == tuple[1]) &
                                       (lossDF['YearID'] == tuple[2])][['CatalogTypeCode', 'ModelCode',
                                                                 'YearID', 'EventID', 'NetOfPreCATLoss',
                                                                 'PostCATNetLoss']].reset_index().drop('index', axis=1)
    sample_lossDF['Recovery'] = sample_lossDF['NetOfPreCATLoss'] - programInfo[1]

    sample_lossDF.loc[sample_lossDF['Recovery']<0, 'Recovery'] = 0
    sample_lossDF.loc[sample_lossDF['Recovery']>programInfo[0], 'Recovery'] = programInfo[0]

    if len(sample_lossDF['Recovery']) == 1:
        sample_lossDF['Recovery'] = min(max(sample_lossDF['Recovery'].values[0] - agg_ret_temp, 0), agg_limit_temp)

    else:
        for i in range(len(sample_lossDF['Recovery'])):

            if sample_lossDF['Recovery'][i] - agg_ret_temp < 0:
                temp_agg_ret = agg_ret_temp - sample_lossDF['Recovery'][i]
            else:
                temp_agg_ret = 0

            sample_lossDF['Recovery'][i] = min(max(sample_lossDF['Recovery'][i] -
                                                   agg_ret_temp, 0), agg_limit_temp)
            agg_ret_temp = copy.deepcopy(temp_agg_ret)
            agg_limit_temp -= copy.deepcopy(sample_lossDF['Recovery'][i])

    sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * (1 - (programInfo[5]))
    sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * programInfo[4]

    return sample_lossDF['Recovery'].values

class ProgramValidation:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.server = server
        self.setup = dbConnection(server)
        self.connection = self.setup.connection
        self.cursor = self.setup.cursor

    def _GetTasks(self, resultDB, resultSID):

        lossDF = self.setup._getLossDF(resultDB, resultSID, 'PORT')
        lossDF.sort(['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID'], inplace=True)

        lossDF = lossDF[['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID', 'NetOfPreCATLoss', 'PostCATNetLoss']]

        Catalogtypes = lossDF.loc[:, 'CatalogTypeCode'].unique()
        task_list = []
        for c in Catalogtypes:
            ModelCode = lossDF.loc[lossDF['CatalogTypeCode'] == c, 'ModelCode'].unique()
            for m in ModelCode:
                yearID = lossDF.loc[(lossDF['CatalogTypeCode'] == c) & (lossDF['ModelCode'] == m) , 'YearID'].unique()
                for y in yearID:
                    task_list.append(tuple([c,m,y]))

        return task_list, lossDF

    def _validate(self, result):


        result.insert(0, 'Status', '-')

        result['DifferencePercent'] = (result['CalculatedPostCATNetLoss'] - result['PostCATNetLoss'])/result['PostCATNetLoss']
        result = result.fillna(0)
        result['DifferencePercent'] = result['DifferencePercent'].astype(int)
        result.loc[abs(result['DifferencePercent']) < 0.1, 'Status'] = 'Pass'
        result.loc[result['Status'] == '-', 'Status'] = 'Fail'

        return result