__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# ToDo-Shashank: Add option for Location and Contract Summary

# Import standard Python packages
import copy
import sys

# Import internal packages
from DbConn.main import *

# Import external Python libraries
import pandas as pd


class LossModValidation:

    def __init__(self, server):

        self.setup = dbConnection(server)
        self.connection = self.setup.connection
        self.cursor = self.setup.cursor

    def _checkRule(self, ModAnalysisSID, perilsAnalysisGrouped, coverage, LOB, occupancy, construction, yearBuilt, stories,
                   contractID, locationID, factor, ModResultSID, resultDB):

        info_analysis = self.setup._getAnalysisInfo(ModAnalysisSID)

        if any(coverage):
            if (not (info_analysis[0][24])) or (info_analysis[0][7] == 'LYR'):

                print('\nWarning: Coverage Option unchecked or Saved by Layer')
                print('\n--------------------------------------------')
                print('..........Validation Stopped.....')
                print('--------------------------------------------')
                sys.exit()

        if any(contractID):

            if not info_analysis[0][7] in ['CON', 'LOC', 'LYR']:

                print('\nWarning: Please run the analysis with the valid Contract, '
                      'Location or Layer as a Save by option')
                print('\n--------------------------------------------')
                print('..........Validation Stopped.....')
                print('--------------------------------------------')
                sys.exit()

        if any(locationID) or any(yearBuilt) or any(stories) or any(construction) or any(occupancy):
            if not info_analysis[0][7] in ['LOC']:

                print('\nWarning: Please run the analysis with the valid Location or '
                      'Location Summary as a Save by option')
                print('\n--------------------------------------------')
                print('..........Validation Stopped.....')
                print('--------------------------------------------')
                sys.exit()

        if any(LOB):
            if not info_analysis[0][7] in ['EA', 'CON', 'LOC', 'LYR']:
                print('\nWarning: Please run the analysis with the valid LOB Option Checked')
                print('\n--------------------------------------------')
                print('..........Validation Stopped.....')
                print('--------------------------------------------')
                sys.exit()

        """
        Summarizing the template info based on Save by option

            +---------------+---------------+-----------+-----------+-------+---------------+---------------+-----------+
            | SaveBy Option | Grouped Perils| Factor    | Coverage  | LOB   | ContractSID   | LocationSID   | LayerID   |
            +---------------+---------------+-----------+-----------+-------+---------------+---------------+-----------+
            | Default       | 1             | 1         | 1         |       |               |               |           |
            | LOB           | 1             | 1         | 1         | 1     |               |               |           |
            | Contract      | 1             | 1         | 1         | 1     | 1             |               |           |
            | Location      | 1             | 1         | 1         | 1     | 1             | 1             |           |
            | Layer         | 1             | 1         |           | 1     | 1             |               | 1         |
            +---------------+---------------+-----------+-----------+-------+---------------+---------------+-----------+

        """
        if info_analysis[0][7] == 'EA':
            LOB_Updt = []
            if any(LOB):
                for i in range(len(LOB)):
                    self.cursor.execute('Select ExposureAttributeSID from [' + resultDB + '].dbo.t' +
                                    str(ModResultSID) + '_LOSS_DimExposureAttribute WHERE ExposureAttribute ' + str(LOB[i]))
                    info = copy.deepcopy(self.cursor.fetchall())
                    print(info)
                    LOB_Updt.append([info[i][0] for i in range(len(info))])
            else:
                for i in range(len(factor)):
                    self.cursor.execute('Select ExposureAttributeSID from [' + resultDB + '].dbo.t' +
                                    str(ModResultSID) + '_LOSS_DimExposureAttribute')
                    info = copy.deepcopy(self.cursor.fetchall())
                    LOB_Updt.append([info[i][0] for i in range(len(info))])
            if any(coverage):
                template_info = zip(perilsAnalysisGrouped, LOB_Updt, factor, coverage)
            else:
                template_info = zip(perilsAnalysisGrouped, LOB_Updt, factor, coverage)

        if info_analysis[0][7] in ['CON', 'LOC']:
            contractID_updt = []
            if any(LOB) and any(contractID):
                for i in range(len(LOB)):
                    try:
                        self.cursor.execute('Select ContractSID from [' + resultDB + '].dbo.t' +
                                        str(ModResultSID) + '_LOSS_DimContract WHERE UserLineOfBusiness ' +
                                        str(LOB[i]) + 'AND ContractID in ' + str(tuple(contractID[i].split(','))))
                    except:
                        self.cursor.execute('Select ContractSID from [' + resultDB + '].dbo.t' +
                                        str(ModResultSID) + '_LOSS_DimContract WHERE UserLineOfBusiness ' +
                                        str(LOB[i]) + 'AND ContractID = ' + "'" +
                                                str(contractID[i]) + "'")

                    info = copy.deepcopy(self.cursor.fetchall())
                    contractID_updt.append([info[i][0] for i in range(len(info))])

            elif any(LOB):
                for i in range(len(LOB)):

                    self.cursor.execute('Select ContractSID from [' + resultDB + '].dbo.t' +
                                    str(ModResultSID) + '_LOSS_DimContract WHERE UserLineOfBusiness ' +
                                    str(LOB[i]))
                    info = copy.deepcopy(self.cursor.fetchall())
                    contractID_updt.append([info[i][0] for i in range(len(info))])

            elif any(contractID):
                for i in range(len(contractID)):
                    try:
                        self.cursor.execute('Select ContractSID from [' + resultDB + '].dbo.t' +
                                        str(ModResultSID) + '_LOSS_DimContract WHERE ContractID in ' + str(tuple(contractID[i].split(','))))
                    except:
                        self.cursor.execute('Select ContractSID from [' + resultDB + '].dbo.t' +
                                        str(ModResultSID) + '_LOSS_DimContract WHERE ContractID  = ' + "'" +
                                                str(contractID[i]) + "'")

                    info = copy.deepcopy(self.cursor.fetchall())
                    contractID_updt.append([info[i][0] for i in range(len(info))])
            else: # With only Perils + Factor case
                for i in range(len(perilsAnalysisGrouped)):
                    self.cursor.execute('Select ContractSID from [' + resultDB + '].dbo.t' +
                                        str(ModResultSID) + '_LOSS_DimContract')
                    info = copy.deepcopy(self.cursor.fetchall())
                    contractID_updt.append([info[i][0] for i in range(len(info))])

            if info_analysis[0][7] == 'CON':
                if any(coverage):
                    template_info = zip(perilsAnalysisGrouped, contractID_updt, factor, coverage)
                else:
                    template_info = zip(perilsAnalysisGrouped, contractID_updt, factor)
            else:
                locationID_updt = []
                if not (any(yearBuilt) and any(stories) and (construction) and (occupancy) and any(locationID)):
                    for i in range(len(contractID_updt)):
                        try:
                            self.cursor.execute('Select LocationSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                                '_LOSS_DimLocation WHERE ContractSID in ' + str(tuple(contractID_updt[i])))
                        except:
                            self.cursor.execute('Select LocationSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                                '_LOSS_DimLocation WHERE ContractSID = ' + "'" +
                                                str(contractID_updt[i][0]) + "'")
                        info = copy.deepcopy(self.cursor.fetchall())
                        locationID_updt.append([info[i][0] for i in range(len(info))])
                        if any(coverage):
                            template_info = zip(perilsAnalysisGrouped, locationID_updt, factor, coverage)
                        else:
                            template_info = zip(perilsAnalysisGrouped, locationID_updt, factor)

                else:
                    if not (any(yearBuilt) and any(stories) and (construction) and (occupancy)):
                        if any(locationID):
                            print('a')
                            locationID_updt = []
                            for i in range(len(locationID)):
                                try:
                                    self.cursor.execute('Select LocationSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                                        '_LOSS_DimLocation WHERE LocationID in ' + str(tuple(locationID[i].split(','))))
                                except:
                                    self.cursor.execute('Select LocationSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                                        '_LOSS_DimLocation WHERE LocationID = ' + "'" +
                                                            str(locationID[i].split(',')[0]) + "'")

                                info = copy.deepcopy(self.cursor.fetchall())
                                locationID_updt.append([info[i][0] for i in range(len(info))])
                            template_info = zip(perilsAnalysisGrouped, locationID_updt, factor)
                    else:
                        script = ('Select * from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                  '_LOSS_DimLocation')
                        dimLocation_DF = pd.read_sql(script, self.connection)
                        print(dimLocation_DF['Stories'])
                        locationID_updt = []
                        for i in range(len(perilsAnalysisGrouped)):
                            if any(construction):
                                try:
                                    if not construction[i] == None:
                                        dimLocation_DF = copy.deepcopy(dimLocation_DF.loc[dimLocation_DF['AIRConstructionCode'] == construction[i], :])
                                except:
                                    dimLocation_DF = copy.deepcopy(dimLocation_DF)
                            if any(occupancy):
                                if not occupancy[i] == None:
                                    dimLocation_DF = copy.deepcopy(dimLocation_DF.loc[dimLocation_DF['AIROccupancyCode'] == occupancy[i], :])
                            if any(yearBuilt):
                                if not yearBuilt[i] == None:
                                    dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['YearBuilt'] == yearBuilt[i], :]
                            if any(stories):
                                if not stories[i]== None:
                                    dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['Stories'] == stories[i], :]

                            locationID_updt.append(dimLocation_DF['LocationSID'].values)
                            script = ('Select * from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                  '_LOSS_DimLocation')
                            dimLocation_DF = pd.read_sql(script, self.connection)
                        if any(coverage):
                            template_info = zip(perilsAnalysisGrouped, locationID_updt, factor, coverage)
                        else:
                            template_info = zip(perilsAnalysisGrouped, locationID_updt, factor)

        if (info_analysis[0][7] == 'PORT') and (info_analysis[0][8] in ['LOCSUM', 'CONSUM']):
            # The reason being, each contract may have multiple perils and it is difficult to track the factor
            if not len(perilsAnalysisGrouped) == 1:
                print('Currently, Peril+Factor template with save by Location Summary/Contract '
                      'Summary is only available when analysis is run by single peril')
                sys.exit()
            else:
                if any(contractID):
                    contractID_updt = []
                    for i in range(len(contractID)):
                        self.cursor.execute('Select ContractSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                            '_LOSS_DimContract where ContractID in ' + str(tuple(contractID[i].split(','))))
                        info = copy.deepcopy(self.cursor.fetchall())
                        contractID_updt.append([info[i][0] for i in range(len(info))])
                else:
                    self.cursor.execute('Select ContractSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                        '_LOSS_DimContract')
                    info = copy.deepcopy(self.cursor.fetchall())
                    contractID_updt = [info[i][0] for i in range(len(info))]
                if info_analysis[0][8] == 'CONSUM':
                    if any(coverage):
                        template_info = zip(perilsAnalysisGrouped, contractID_updt, factor, coverage)
                    else:
                        template_info = zip(perilsAnalysisGrouped, contractID_updt, factor)
                else:
                    if not (any(yearBuilt) or any(stories) or (construction) or (occupancy)):
                        if any(locationID):
                            locationID_updt = []
                            for i in range(len(contractID_updt)):
                                try:
                                    self.cursor.execute('Select LocationSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                                        '_LOSS_DimLocation WHERE ContractSID in ' + str(tuple(contractID_updt[i])))
                                except:
                                    self.cursor.execute('Select LocationSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                                        '_LOSS_DimLocation WHERE ContractSID = ' + "'" +
                                                            str(contractID_updt[i][0]) + "'")
                                info = copy.deepcopy(self.cursor.fetchall())
                                locationID_updt.append([info[i][0] for i in range(len(info))])
                    else:
                        query = ('Select * from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                            '_LOSS_DimLocation')
                        dimLocation_DF = pd.read_sql(query, self.connection)
                        locationID_updt = []
                        for i in range(len(perilsAnalysisGrouped)):
                            if any(construction):
                                try:
                                    if not construction[i] == None:
                                        dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['AIRConstructionCode'] == construction[i], :]
                                except:
                                    dimLocation_DF = dimLocation_DF
                            if any(occupancy):
                                if not occupancy[i] == None:
                                    dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['AIROccupancyCode'] == occupancy[i], :]
                            if any(yearBuilt):
                                if not yearBuilt[i] == None:
                                    dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['YearBuilt'] == yearBuilt[i], :]
                            if any(stories):
                                if not stories[i]== None:
                                    dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['Stories'] == stories[i], :]

                            dimLocation_DF_copy = copy.deepcopy(dimLocation_DF)
                            locationID_updt.append(dimLocation_DF['LocationSID'].values)
                        if any(coverage):
                            template_info = zip(perilsAnalysisGrouped, locationID_updt, factor)
                        else:
                            template_info = zip(perilsAnalysisGrouped, locationID_updt, factor, coverage)
        if (info_analysis[0][7] == 'PORT'):
            if any(coverage):
                template_info = zip(perilsAnalysisGrouped, factor, coverage)
            else:
                template_info = zip(perilsAnalysisGrouped, factor)
        print(template_info)
        return template_info

    def _getLossDF(self, ModAnalysisSID, resultDB, BaseResultSID, ModResultSID, coverage):

        info_analysis = self.setup._getAnalysisInfo(ModAnalysisSID)
        resultDF = pd.DataFrame()
        resultDF['Status'] = 1
         # In the case where analysis is saved by LOB, get the loss numbers from Loss By Exposure Attribute
        if info_analysis[0][7] == 'EA':

            resultDF_Base = self.setup._getLossDF(resultDB, BaseResultSID, 'LOB')
            resultDF_Mod = self.setup._getLossDF(resultDB, ModResultSID, 'LOB')

        # In the case where analysis is saved by Default, get the loss numbers from Loss By Event
        if info_analysis[0][7] == 'PORT':

            if info_analysis[0][8] == 'CONSUM':

                resultDF_Base = self.setup._getLossDF(resultDB, BaseResultSID, 'CONSUM')
                resultDF_Mod = self.setup._getLossDF(resultDB, ModResultSID, 'CONSUM')

            elif info_analysis[0][8] == 'LOCSUM':

                resultDF_Base = self.setup._getLossDF(resultDB, BaseResultSID, 'LOCSUM')
                resultDF_Mod = self.setup._getLossDF(resultDB, ModResultSID, 'LOCSUM')

            else:

                resultDF_Base = self.setup._getLossDF(resultDB, BaseResultSID, 'Event')
                resultDF_Mod = self.setup._getLossDF(resultDB, ModResultSID, 'Event')

        # In the case where analysis is saved by Contract, get the loss numbers from Loss By Contract
        if info_analysis[0][7] == 'CON':

            resultDF_Base = self.setup._getLossDF(resultDB, BaseResultSID, 'CON')
            resultDF_Mod = self.setup._getLossDF(resultDB, ModResultSID, 'CON')

        # In the case where analysis is saved by Location, get the loss numbers from Loss By Location
        elif info_analysis[0][7] == 'LOC':

            resultDF_Base = self.setup._getLossDF(resultDB, BaseResultSID, 'LOC')
            resultDF_Mod = self.setup._getLossDF(resultDB, ModResultSID, 'LOC')

        # In the case where analysis is saved by Layer, get the loss numbers from Loss By Layer
        elif info_analysis[0][7] == 'LYR':

            resultDF_Base = self.setup._getLossDF(resultDB, BaseResultSID, 'LYR')
            resultDF_Mod = self.setup._getLossDF(resultDB, ModResultSID, 'LYR')

        # PerilSet Code as key only if the analysis wasn't run by LocSummary or ConSummary
        if not info_analysis[0][8] in ['LOCSUM', 'CONSUM']:

            resultDF['PerilSetCode'] = resultDF_Mod['PerilSetCode']

        if info_analysis[0][7] == 'EA':
            resultDF['ExposureAttributeSID'] = resultDF_Mod['ExposureAttributeSID']

        if info_analysis[0][7] == 'CON':
            resultDF['ContractSID'] = resultDF_Mod['ContractSID']

        if info_analysis[0][7] == 'LOC':
            resultDF['LocationSID'] = resultDF_Mod['LocationSID']
            # resultDF['ContractSID'] = resultDF_Mod['ContractSID']

        if info_analysis[0][7] == 'LYR':
            resultDF['LayerSID'] = resultDF_Mod['LayerSID']

        if info_analysis[0][7] == 'GEOL':
            resultDF['GeographySID'] = resultDF_Mod['GeographySID']

        if info_analysis[0][7] == 'CONGEOL':
            resultDF['ContractSID'] = resultDF_Mod['ContractSID']
            resultDF['GeographySID'] = resultDF_Mod['GeographySID']

        if info_analysis[0][8] == 'LOCSUM':
            resultDF['LocationSID'] = resultDF_Mod['LocationSID']

        if info_analysis[0][8] == 'CONSUM':
            resultDF['ContractSID'] = resultDF_Mod['ContractSID']

        resultDF['GroundUpLoss_Base'] = resultDF_Base.loc[:, 'GroundUpLoss']
        resultDF['GroundUpLoss_Mod'] = resultDF_Mod.loc[:, 'GroundUpLoss']
        resultDF['Ratio'] = resultDF_Mod.loc[:, 'GroundUpLoss'] / resultDF_Base.loc[:, 'GroundUpLoss']

        if any(coverage):

            coverages = ['A', 'B', 'C', 'D']
            for i in coverages:
                resultDF['GroundUpLoss' + i + '_Base'] = resultDF_Base.loc[:, 'GroundUpLoss_' + i]
                resultDF['GroundUpLoss' + i + '_Mod'] = resultDF_Mod.loc[:, 'GroundUpLoss_' + i]
                resultDF['GroundUpLoss' + i + '_Ratio'] = resultDF_Mod.loc[:, 'GroundUpLoss_' + i] / resultDF_Base.loc[:, 'GroundUpLoss_' + i]
        else:
            coverages = ['A', 'B', 'C', 'D']
            for i in coverages:
                resultDF['GroundUpLoss' + i + '_Base'] = '-'
                resultDF['GroundUpLoss' + i + '_Mod'] = '-'
                resultDF['GroundUpLoss' + i + '_Ratio'] = '-'
        resultDF.loc[:, 'Status'] = 1

        return resultDF

    def _validate(self, resultDF, template_info, coverage, ModAnalysisSID):

        info_analysis = self.setup._getAnalysisInfo(ModAnalysisSID)

        for i in range(len(template_info)):

            if not info_analysis[0][8] in ['LOCSUM', 'CONSUM']:

                if not any(coverage):
                    if info_analysis[0][7] != 'PORT':

                        resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                     (resultDF.iloc[:, 2].isin(template_info[i][1])), 'Input_Ratio'] = float(template_info[i][2])
                        resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                     (resultDF.iloc[:, 2].isin(template_info[i][1])), 'Difference'] = \
                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) & (resultDF.iloc[:, 2].isin(template_info[i][1])), 'Ratio'] - float(template_info[i][2])

                        if (abs(resultDF[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                            (resultDF.iloc[:, 2].isin(template_info[i][1]))]['Difference']) < 0.001).all():
                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                         (resultDF.iloc[:, 2].isin(template_info[i][1])), 'Status'] = 'Pass'
                        else:
                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                         (resultDF.iloc[:, 2].isin(template_info[i][1])), 'Status'] = 'Fail'


                        if ((resultDF[resultDF['Status'] == 1.0 ]['Ratio'] - 1.0).all() < 0.001).all():
                            resultDF.loc[resultDF['Status'] == 1.0, 'Input_Ratio'] = 1.0
                            resultDF.loc[resultDF['Status'] == 1.0, 'Difference'] = resultDF.loc[resultDF['Status'] == 1.0, 'Ratio'] - 1.0
                            resultDF.loc[resultDF['Status'] == 1.0, 'Status'] = 'Pass'

                    else:

                        resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'Input_Ratio'] = float(template_info[i][1])
                        resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'Difference'] = \
                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) , 'Ratio'] - float(template_info[i][1])

                        if (abs(resultDF[(resultDF['PerilSetCode'].isin(template_info[i][0]))]['Difference']) < 0.001).all():
                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'Status'] = 'Pass'
                        else:
                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'Status'] = 'Fail'

                        if ((resultDF[resultDF['Status'] == 1.0 ]['Ratio'] - 1.0).all() < 0.001).all():
                            resultDF.loc[resultDF['Status'] == 1.0, 'Input_Ratio'] = 1.0
                            resultDF.loc[resultDF['Status'] == 1.0, 'Difference'] = resultDF.loc[resultDF['Status'] == 1.0, 'Ratio'] - 1.0
                            resultDF.loc[resultDF['Status'] == 1.0, 'Status'] = 'Pass'
                else:
                    if info_analysis[0][7] != 'PORT':
                        coverages = list(template_info[i][3])
                        for j in coverages:
                            resultDF.loc[(resultDF['GroundUpLoss' + j + '_Mod'] == 0) & (resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                         (resultDF.iloc[:, 2].isin(template_info[i][1])), 'GroundUpLoss' + j + '_Ratio'] = float(template_info[i][2])

                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                         (resultDF.iloc[:, 2].isin(template_info[i][1])) & (resultDF['GroundUpLoss'+ j + '_Ratio'] != 1), 'Input_Ratio_' + j] = float(template_info[i][2])

                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                         (resultDF.iloc[:, 2].isin(template_info[i][1])) & (resultDF['GroundUpLoss'+ j + '_Ratio'] != 1), 'Difference_' + j] = \
                                resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) & (resultDF.iloc[:, 2].isin(template_info[i][1])) & (resultDF['GroundUpLoss'+ j + '_Ratio'] != 1), 'GroundUpLoss' + j + '_Ratio'] - float(template_info[i][2])

                            if (abs(resultDF[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                (resultDF.iloc[:, 2].isin(template_info[i][1])) & (resultDF['GroundUpLoss'+ j + '_Ratio'] != 1)]['Difference_' + j].fillna(0)) < 0.001).all():
                                resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                                             (resultDF.iloc[:, 2].isin(template_info[i][1])) & (resultDF['GroundUpLoss'+ j + '_Ratio'] != 1), 'Status'] = 'Pass'
                            # else:
                            #     resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])) &
                            #                  (resultDF.iloc[:, 2].isin(template_info[i][1])), 'Status'] = 'Fail'

                            resultDF = resultDF.fillna(1)
                            resultDF.loc[:, 'Difference_' + j] = resultDF.loc[:, 'GroundUpLoss' + j + '_Ratio'] - resultDF.loc[:, 'Input_Ratio_' + j]
                            resultDF.loc[resultDF['Difference_' + j] < 0.001, 'Status'] = 'Pass'
                    else:
                        coverages = list(template_info[i][2])
                        for j in coverages:
                            resultDF.loc[resultDF['GroundUpLoss' + j + '_Mod'] == 0, 'GroundUpLoss' + j + '_Ratio'] = float(template_info[i][1])
                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'Input_Ratio_' + j] = float(template_info[i][1])

                            resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'Difference_' + j] = \
                                resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'GroundUpLoss' + j + '_Ratio'] - float(template_info[i][1])

                            if (abs(resultDF[(resultDF['PerilSetCode'].isin(template_info[i][0]))]['Difference_' + j]) < 0.001).all():
                                resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'Status'] = 'Pass'
                            # else:
                            #     resultDF.loc[(resultDF['PerilSetCode'].isin(template_info[i][0])), 'Status'] = 'Fail'

                            resultDF = resultDF.fillna(1)
                            resultDF.loc[:, 'Difference_' + j] = resultDF.loc[:, 'GroundUpLoss' + j + '_Ratio'] - resultDF.loc[:, 'Input_Ratio_' + j]
                            resultDF.loc[resultDF['Difference_' + j] < 0.001, 'Status'] = 'Pass'

            elif info_analysis[0][8] == 'LOCSUM':

                resultDF.loc[(resultDF['LocationSID'].isin(template_info[i][0])), 'Input_Ratio'] = float(template_info[i][1])
                resultDF.loc[(resultDF['LocationSID'].isin(template_info[i][0])), 'Difference'] = \
                    resultDF.loc[(resultDF['LocationSID'].isin(template_info[i][0])) , 'Ratio'] - float(template_info[i][1])

                if (abs(resultDF[(resultDF['LocationSID'].isin(template_info[i][0]))]['Difference']) < 0.001).all():
                    resultDF.loc[(resultDF['LocationSID'].isin(template_info[i][0])), 'Status'] = 'Pass'
                else:
                    resultDF.loc[(resultDF['LocationSID'].isin(template_info[i][0])), 'Status'] = 'Fail'

                if ((resultDF[resultDF['Status'] == 1.0 ]['Ratio'] - 1.0).all() < 0.001).all():
                    resultDF.loc[resultDF['Status'] == 1.0, 'Input_Ratio'] = 1.0
                    resultDF.loc[resultDF['Status'] == 1.0, 'Difference'] = resultDF.loc[resultDF['Status'] == 1.0, 'Ratio'] - 1.0
                    resultDF.loc[resultDF['Status'] == 1.0, 'Status'] = 'Pass'

            elif info_analysis[0][8] == 'CONSUM':

                resultDF.loc[(resultDF['ContractSID'].isin(template_info[i][0])), 'Input_Ratio'] = float(template_info[i][1])
                resultDF.loc[(resultDF['ContractSID'].isin(template_info[i][0])), 'Difference'] = \
                    resultDF.loc[(resultDF['ContractSID'].isin(template_info[i][0])) , 'Ratio'] - float(template_info[i][1])

                if (abs(resultDF[(resultDF['ContractSID'].isin(template_info[i][0]))]['Difference']) < 0.001).all():
                    resultDF.loc[(resultDF['ContractSID'].isin(template_info[i][0])), 'Status'] = 'Pass'
                else:
                    resultDF.loc[(resultDF['ContractSID'].isin(template_info[i][0])), 'Status'] = 'Fail'

                if ((resultDF[resultDF['Status'] == 1.0 ]['Ratio'] - 1.0).all() < 0.001).all():
                    resultDF.loc[resultDF['Status'] == 1.0, 'Input_Ratio'] = 1.0
                    resultDF.loc[resultDF['Status'] == 1.0, 'Difference'] = resultDF.loc[resultDF['Status'] == 1.0, 'Ratio'] - 1.0
                    resultDF.loc[resultDF['Status'] == 1.0, 'Status'] = 'Pass'

        resultDF.loc[resultDF['Status'] == 1.0, 'Status'] = 'Fail'
        if info_analysis[0][7] == 'PORT':
            resultDF.rename(columns={str(resultDF.columns.values[1]): 'ID'}, inplace=True)

        else:
            resultDF.insert(1, "ID", [str(resultDF.iloc[:, 1].values[i]) + '_' + str(resultDF.iloc[:, 2].values[i]) for i in range(len(resultDF.iloc[:, 1]))])
            resultDF.drop(resultDF.columns[[2, 3]], axis=1, inplace=True)

        return resultDF