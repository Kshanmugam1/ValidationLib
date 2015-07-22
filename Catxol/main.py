__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# Import standard Python packages
import copy
import sys
import threading
import multiprocessing as mp
import math
from Queue import *
from functools import partial


# Import internal packages
from DbConn.main import *

# Import external Python libraries
import pandas as pd

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def _validate(tuple, lossDF, occ_ret, occ_limit, agg_ret, agg_limit, placed_percent, ins_coins):

    agg_limit_temp = copy.deepcopy(agg_limit[tuple[0]])
    agg_ret_temp = copy.deepcopy(agg_ret[tuple[0]])

    sample_lossDF = lossDF.loc[(lossDF['CatalogTypeCode'] == tuple[1]) & (lossDF['ModelCode'] == tuple[2]) &
                                       (lossDF['YearID'] == tuple[3])][['CatalogTypeCode', 'ModelCode',
                                                                 'YearID', 'EventID', 'NetOfPreCATLoss',
                                                                 'PostCATNetLoss']].reset_index().drop('index', axis=1)
    sample_lossDF['Recovery'] = sample_lossDF['NetOfPreCATLoss'] - occ_ret[tuple[0]]

    sample_lossDF.loc[sample_lossDF['Recovery']<0, 'Recovery'] = 0
    sample_lossDF.loc[sample_lossDF['Recovery']>occ_limit[tuple[0]], 'Recovery'] = occ_limit[tuple[0]]

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

    sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * (1 - float(ins_coins[tuple[0]]))
    sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * placed_percent[tuple[0]]

    sample_lossDF['CalculatedPostCATNetLoss'] = sample_lossDF['NetOfPreCATLoss'] - \
                                                sample_lossDF['Recovery']
    return sample_lossDF

class ProgramValidation:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.server = server
        self.setup = dbConnection(server)
        self.connection = self.setup.connection
        self.cursor = self.setup.cursor
        self.lock = threading.RLock()

    def _GetTasks(self, resultDB, resultSID, occ_limit):

        print(resultSID)
        lossDF = self.setup._getLossDF(resultDB, resultSID, 'PORT')
        lossDF.sort(['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID'], inplace=True)

        lossDF = lossDF[['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID', 'NetOfPreCATLoss', 'PostCATNetLoss']]

        Catalogtypes = lossDF.loc[:, 'CatalogTypeCode'].unique()
        task_list = []
        for i in range(len(occ_limit)):
            for c in Catalogtypes:
                ModelCode = lossDF.loc[lossDF['CatalogTypeCode'] == c, 'ModelCode'].unique()
                for m in ModelCode:
                    yearID = lossDF.loc[(lossDF['CatalogTypeCode'] == c) & (lossDF['ModelCode'] == m) , 'YearID'].unique()
                    for y in yearID:
                        task_list.append(tuple([i,c,m,y]))

        return task_list, lossDF

    def _getResultDF(self, result):

        resultDF = pd.DataFrame(columns=['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID',
                                         'NetOfPreCATLoss', 'PostCATNetLoss', 'Recovery',
                                         'CalculatedPostCATNetLoss'])
        for i in range(len(result)):
            resultDF = pd.concat([resultDF, result[i]], axis=0)

        resultDF.insert(0, 'Status', '-')
        resultDF.loc[resultDF['PostCATNetLoss'] - resultDF['CalculatedPostCATNetLoss'] < 0.001, 'Status'] = 'Pass'
        resultDF.loc[resultDF['Status'] == '-', 'Status'] = 'Fail'

        return resultDF