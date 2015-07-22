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

# Import internal packages
from DbConn.main import *
import threading
# Import external Python libraries
import pandas as pd

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]

class ProgramValidation:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.setup = dbConnection(server)
        self.connection = self.setup.connection
        self.cursor = self.setup.cursor
        self.lock = threading.RLock()

    def _validate(self, resultDB, resultSID, occ_limit, occ_ret, agg_limit, agg_ret, placed_percent, ins_coins, tolerance):

        self.resultDF = pd.DataFrame(columns=['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID',
                                              'NetOfPreCATLoss', 'PostCATNetLoss', 'Recovery',
                                              'CalculatedPostCATNetLoss'])

        def multiThread(y, m, c):

            agg_limit_temp = copy.deepcopy(agg_limit)
            agg_ret_temp = copy.deepcopy(agg_ret)
            sample_lossDF = lossDF.loc[(lossDF['CatalogTypeCode'] == c) & (lossDF['ModelCode'] == m) &
                                       (lossDF['YearID'] == y)][['CatalogTypeCode', 'ModelCode',
                                                                 'YearID', 'EventID', 'NetOfPreCATLoss',
                                                                 'PostCATNetLoss']].reset_index().drop('index', axis=1)


            sample_lossDF['Recovery'] = sample_lossDF['NetOfPreCATLoss'] - occ_ret

            sample_lossDF.loc[sample_lossDF['Recovery']<0, 'Recovery'] = 0
            sample_lossDF.loc[sample_lossDF['Recovery']>occ_limit, 'Recovery'] = occ_limit

            if len(sample_lossDF['Recovery']) == 1:
                sample_lossDF['Recovery'] = min(max(sample_lossDF['Recovery'].values[0] - agg_ret, 0), agg_limit)

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

            sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * (1 - ins_coins[0])
            sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * placed_percent

            sample_lossDF['CalculatedPostCATNetLoss'] = sample_lossDF['NetOfPreCATLoss'] - \
                                                        sample_lossDF['Recovery']
            self.lock.acquire(blocking=1)
            self.resultDF = pd.concat([self.resultDF, sample_lossDF], axis=0)
            self.lock.release()

        lossDF = self.setup._getLossDF(resultDB, resultSID, 'PORT')
        lossDF.sort(['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID'], inplace=True)

        lossDF = lossDF[['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID', 'NetOfPreCATLoss', 'PostCATNetLoss']]

        Catalogtypes = lossDF.loc[:, 'CatalogTypeCode'].unique()
        thread_list = []
        for c in Catalogtypes:
            ModelCode = lossDF.loc[lossDF['CatalogTypeCode'] == c, 'ModelCode'].unique()
            for m in ModelCode:
                yearID = lossDF.loc[(lossDF['CatalogTypeCode'] == c) & (lossDF['ModelCode'] == m) , 'YearID'].unique()
                for y in yearID:
                    t = threading.Thread(target=multiThread, args=(y, m, c))
                    thread_list.append(t)

        thread_list = chunks(thread_list, 600)
        for i in range(len(thread_list)):
            for thread in thread_list[i]:
                thread.start()

            for thread in thread_list[i]:
                thread.join()

        self.resultDF.insert(0, 'Status', '-')
        self.resultDF.loc[self.resultDF['PostCATNetLoss'] - self.resultDF['CalculatedPostCATNetLoss'] < 0.001, 'Status'] = 'Pass'
        self.resultDF.loc[self.resultDF['Status'] == '-', 'Status'] = 'Fail'

        return self.resultDF