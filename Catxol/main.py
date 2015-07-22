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
import time
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

        def multiThread(y, m, c, j):

            agg_limit_temp = copy.deepcopy(agg_limit[j])
            agg_ret_temp = copy.deepcopy(agg_ret[j])

            sample_lossDF = lossDF.loc[(lossDF['CatalogTypeCode'] == c) & (lossDF['ModelCode'] == m) &
                                       (lossDF['YearID'] == y)][['CatalogTypeCode', 'ModelCode',
                                                                 'YearID', 'EventID', 'NetOfPreCATLoss',
                                                                 'PostCATNetLoss']].reset_index().drop('index', axis=1)


            sample_lossDF['Recovery'] = sample_lossDF['NetOfPreCATLoss'] - occ_ret[j]

            sample_lossDF.loc[sample_lossDF['Recovery']<0, 'Recovery'] = 0
            sample_lossDF.loc[sample_lossDF['Recovery']>occ_limit[j], 'Recovery'] = occ_limit[j]

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

            sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * (1 - float(ins_coins[j]))
            sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * placed_percent[j]

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
        for i in range(len(occ_limit)):
            for c in Catalogtypes:
                ModelCode = lossDF.loc[lossDF['CatalogTypeCode'] == c, 'ModelCode'].unique()
                for m in ModelCode:
                    yearID = lossDF.loc[(lossDF['CatalogTypeCode'] == c) & (lossDF['ModelCode'] == m) , 'YearID'].unique()
                    for y in yearID:
                        t = threading.Thread(target=multiThread, args=(y, m, c, i))
                        thread_list.append(t)
                        try:
                            t.start()
                        except:
                            time.sleep(1)
                            t.start()

        for thread in thread_list:
            thread.join()

        self.resultDF.insert(0, 'Status', '-')
        self.resultDF.loc[self.resultDF['PostCATNetLoss'] - self.resultDF['CalculatedPostCATNetLoss'] < 0.001, 'Status'] = 'Pass'
        self.resultDF.loc[self.resultDF['Status'] == '-', 'Status'] = 'Fail'

        return self.resultDF