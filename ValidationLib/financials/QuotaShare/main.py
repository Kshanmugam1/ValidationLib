__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# Import internal packages
from ValidationLib.database.main import *

def _getRecovery(tuple, lossDF, programInfo):

    agg_limit_temp = copy.deepcopy(programInfo[1])

    sample_lossDF = lossDF.loc[(lossDF['CatalogTypeCode'] == tuple[0]) & (lossDF['ModelCode'] == tuple[1]) &
                                       (lossDF['YearID'] == tuple[2])][['CatalogTypeCode', 'ModelCode',
                                                                 'YearID', 'EventID', 'GrossLoss',
                                                                 'NetOfPreCATLoss', 'TotalPerRiskReRecoveryLoss']].reset_index().drop('index', axis=1)

    sample_lossDF['CalculatedTotalPerRiskReRecoveryLoss'] = sample_lossDF['GrossLoss'] * programInfo[2] * programInfo[3]
    sample_lossDF['CalculatedRecovery'] = copy.deepcopy(sample_lossDF['CalculatedTotalPerRiskReRecoveryLoss'])

    sample_lossDF.loc[sample_lossDF['CalculatedRecovery']>programInfo[0], 'CalculatedRecovery'] = programInfo[0]

    if len(sample_lossDF['CalculatedRecovery']) == 1:

        sample_lossDF['CalculatedRecovery'] = min(sample_lossDF['CalculatedRecovery'].values[0], agg_limit_temp)

    else:
        for i in range(len(sample_lossDF['CalculatedRecovery'])):

            sample_lossDF['CalculatedRecovery'][i] = min(sample_lossDF['CalculatedRecovery'][i], agg_limit_temp)
            agg_limit_temp -= copy.deepcopy(sample_lossDF['CalculatedRecovery'][i])
    sample_lossDF['CalculatedRecovery'] = copy.deepcopy(sample_lossDF['CalculatedRecovery'] * programInfo[3])
    return sample_lossDF[['CalculatedTotalPerRiskReRecoveryLoss', 'CalculatedRecovery']].values.tolist()


class QSValidation:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.server = server
        self.setup = Database(server)
        self.connection = self.setup.connection
        self.cursor = self.setup.cursor

    def _GetTasks(self, resultDB, resultSID):

        lossDF = self.setup._getLossDF(resultDB, resultSID, 'PORT')
        lossDF.sort(['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID'], inplace=True)

        lossDF = lossDF[['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID',
                         'GrossLoss', 'NetOfPreCATLoss', 'TotalPerRiskReRecoveryLoss']]

        Catalogtypes = lossDF.loc[:, 'CatalogTypeCode'].unique()
        task_list = []
        for c in Catalogtypes:
            ModelCode = lossDF.loc[lossDF['CatalogTypeCode'] == c, 'ModelCode'].unique()
            for m in ModelCode:
                yearID = lossDF.loc[(lossDF['CatalogTypeCode'] == c) & (lossDF['ModelCode'] == m) , 'YearID'].unique()
                for y in yearID:
                    task_list.append(tuple([c,m,y]))

        return task_list, lossDF