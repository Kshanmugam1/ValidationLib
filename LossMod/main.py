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

# Import external Python libraries
import pandas as pd


class LossModValidation:

    def __init__(self, server):

        self.setup = dbConnection(server)
        self.connection = self.setup.connection
        self.cursor = self.setup.cursor

    def _getLossModInfo(self, LossModTempID):

        script = 'SELECT * FROM AIRUserSetting.dbo.tLossModTemplateRule ' \
                 'WHERE LossModTemplateSID = ' + str(LossModTempID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())

        coverage = []
        factor = []
        LOB = []
        contractID = []
        locationID = []
        yearBuilt = []
        stories = []
        construction = []
        occupancy = []
        perilsTemp = []

        for  i in range (len(info)):

            coverage.append(info[i][9])
            factor.append(info[i][4])
            LOB.append(info[i][6])
            contractID.append(info[i][13])
            locationID.append(info[i][14])
            yearBuilt.append(info[i][11])
            stories.append(info[i][12])
            perilsTemp.append(info[i][2])

        script = 'Select AIROccupancyCode from AIRUserSetting.dbo.tLossModTemplateRuleOccupancyCode ' \
                 'where LossModTemplateSID = ' + str(LossModTempID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        if info:
            for i in range (len(info)):
                occupancy.append(info[i][0])

        script = 'Select AIRConstructionCode from AIRUserSetting.dbo.tLossModTemplateRuleConstructionCode ' \
                 'where LossTemplateSID = ' + str(LossModTempID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        if info:
            for i in range (len(info)):
                construction.append(info[i][0])

        return perilsTemp, coverage, LOB, occupancy, construction, yearBuilt, stories, contractID, locationID, factor

    def _groupAnalysisPerils(self, ModAnalysisSID, perilsTemp):

        perilsAnalysis = self.setup._getPerilsAnalysis(ModAnalysisSID)

        script = 'Select PerilSet FROM AIRReference.dbo.tPerilSet WHERE PerilSetCode = ' + str(perilsAnalysis)
        self.cursor.execute(script)
        perilsAnalyisSeperateList =  copy.deepcopy(self.cursor.fetchall()[0][0].split(', '))
        perilsAnalysisGrouped = []

        for i in range(len(perilsTemp)):

            script = 'Select PerilSet FROM AIRReference.dbo.tPerilSet WHERE PerilSetCode = ' + str(perilsTemp[i])
            self.cursor.execute(script)
            perilsTemplateList = copy.deepcopy(self.cursor.fetchall()[0][0].split(', '))

            perils_exclusive = [perilsTemplateList[i] for i in range(len(perilsTemplateList))
                                if perilsTemplateList[i] in perilsAnalyisSeperateList]
            try:
              valid_grouped_perils = [perils_exclusive[i] for i in range(len(perils_exclusive))
                                        if perils_exclusive[i] in ['EQ', 'FF', 'SL', 'TS', 'PF', 'SU', 'TC']]
              script = 'Select PerilDisplayGroup, Sum(PerilSetCode) FROM AIRReference.dbo.tPeril WHERE PerilCode IN' \
                       + str(tuple(valid_grouped_perils)) + 'Group BY PerilDisplayGroup'
              self.cursor.execute(script)
              info = copy.deepcopy(self.cursor.fetchall())
              perils = [info[i][1] for  i in range(len(info))]

            except:
                script = 'Select PerilSetCode FROM AIRReference.dbo.tPeril WHERE PerilCode = '+ "'" + str(perils_exclusive[0]) + "'"

                self.cursor.execute(script)
                info = copy.deepcopy(self.cursor.fetchall())
                perils = [info[i][0] for  i in range(len(info))]


            perilsAnalysisGrouped.append(perils)

        return perilsAnalysisGrouped

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
                for i in range(factor):
                    self.cursor.execute('Select ExposureAttributeSID from [' + resultDB + '].dbo.t' +
                                    str(ModResultSID) + '_LOSS_DimExposureAttribute')
                    info = copy.deepcopy(self.cursor.fetchall())
                    LOB_Updt[i] = [info[i][0] for i in range(len(info))]

            template_info = zip(perilsAnalysisGrouped, LOB_Updt, factor)

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
                 template_info = zip(perilsAnalysisGrouped, contractID_updt, factor)
            else:
                locationID_updt = []
                if not (any(yearBuilt) and any(stories) and (construction) and (occupancy) and any(locationID)):
                    for i in range(len(contractID_updt)):
                        try:
                            self.cursor.execute('Select LocationSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                                '_LOSS_DimLocation WHERE ContractSID in ' + str(tuple(contractID_updt[i])))
                        except:
                            print(contractID_updt)
                            self.cursor.execute('Select LocationSID from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                                '_LOSS_DimLocation WHERE ContractSID = ' + "'" +
                                                str(contractID_updt[i][0]) + "'")
                        info = copy.deepcopy(self.cursor.fetchall())
                        locationID_updt.append([info[i][0] for i in range(len(info))])
                else:
                    if not (any(yearBuilt) and any(stories) and (construction) and (occupancy)):
                        if any(locationID):
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
                                print((locationID[i].split(',')))
                                locationID_updt.append([info[i][0] for i in range(len(info))])
                    else:
                        script = ('Select * from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                  '_LOSS_DimLocation')
                        dimLocation_DF = pd.read_sql(script, self.connection)
                        locationID_updt = []
                        for i in range(len(perilsAnalysisGrouped)):

                            if any(construction):
                                dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['AIRConstructionCode'].isin(construction[i])][:]
                            if any(occupancy):
                                dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['AIROccupancyCode'].isin(occupancy[i])][:]
                            if any(yearBuilt):
                                dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['YearBuilt'].isin(yearBuilt[i])][:]
                            if any(stories):
                                dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['Stories'].isin(stories[i])][:]

                            locationID_updt.append(dimLocation_DF['LocationSID'].values)
                            script = ('Select * from [' + resultDB + '].dbo.t' + str(ModResultSID) +
                                  '_LOSS_DimLocation')
                            dimLocation_DF = pd.read_sql(script, self.connection)
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
                                dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['AIRConstructionCode'].isin(construction[i])][:]
                            if any(occupancy):
                                dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['AIROccupancyCode'].isin(occupancy[i])][:]
                            if any(yearBuilt):
                                dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['YearBuilt'].isin(yearBuilt[i])][:]
                            if any(stories):
                                dimLocation_DF = dimLocation_DF.loc[dimLocation_DF['Stories'].isin(stories[i])][:]

                            dimLocation_DF_copy = copy.deepcopy(dimLocation_DF)
                            locationID_updt.append(dimLocation_DF['LocationSID'].values)
                        template_info = zip(perilsAnalysisGrouped, locationID_updt, factor)

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
                resultDF['GroundUpLoss' + i + '_Mod_Ratio'] = resultDF_Mod.loc[:, 'GroundUpLoss_' + i] / resultDF_Base.loc[:, 'GroundUpLoss_' + i]
        else:
            coverages = ['A', 'B', 'C', 'D']
            for i in coverages:
                resultDF['GroundUpLoss' + i + '_Base'] = '-'
                resultDF['GroundUpLoss' + i + '_Mod'] = '-'
                resultDF['GroundUpLoss' + i + '_Mod_Ratio'] = '-'
        resultDF.loc[:, 'Status'] = 1

        return resultDF

    def _validate(self, resultDF, template_info, coverage, ModAnalysisSID):

        info_analysis = self.setup._getAnalysisInfo(ModAnalysisSID)

        for i in range(len(template_info)):

            if not info_analysis[0][8] in ['LOCSUM', 'CONSUM']:
                if not any(coverage):

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

        resultDF.loc[resultDF['Status'] == 1.0, 'Status'] = 'Fail'

        return resultDF