# Import standard Python packages
import copy

# Import external Python libraries
import pyodbc
import copy
import pandas as pd


class dbConnection:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.connection = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server)
        self.cursor = self.connection.cursor()

    def _getResultSID(self, AnalysisSID):

        """
        The function _getResultSID is a method of class dbConnection. Given the analysis_sid, it returns the associated
        result_sid
        """

        script = 'SELECT ResultSID FROM AIRProject.dbo.tAnalysis ' \
                 'WHERE AnalysisSID = ' + str(AnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        resultSID = info[0][0]

        return resultSID

    def _getBaseAnalysisSID(self, ModAnalysisSID):

        script = 'SELECT BaseAnalysisSID FROM AIRProject.dbo.tLossAnalysisOption ' \
                 'WHERE AnalysisSID = ' + str(ModAnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        baseAnalysisSID = info[0][0]

        return baseAnalysisSID

    def _getLossModTemID(self, ModAnalysisSID):

        script = 'SELECT LossModTemplateSID FROM AIRProject.dbo.tLossAnalysisOption ' \
                 'WHERE AnalysisSID = ' + str(ModAnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        LossModTempID = info[0][0]

        return LossModTempID

    def _getPerilsAnalysis(self, ModAnalysisSID):

        script = 'SELECT PerilSetCode FROM AIRProject.dbo.tLossAnalysisOption ' \
                 'WHERE AnalysisSID = ' + str(ModAnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        PerilsAnalysis = info[0][0]

        return PerilsAnalysis

    def _getAnalysisInfo(self, AnalysisSID):

        script =  'SELECT * FROM AIRProject.dbo.tLossAnalysisOption ' \
                  'WHERE AnalysisSID = ' + str(AnalysisSID)
        self.cursor.execute(script)

        return (copy.deepcopy(self.cursor.fetchall()))

    def _getLossDF(self, resultDB, resultSID, type):


        if type in ['EA', 'LOB']:

            script = 'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByExposureAttribute'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['CONSUM', 'Contract Summary']:

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByContractSummary'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['LOCSUM', 'Location Summary']:

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByLocationSummary'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['PORT', 'Event']:

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['CON', 'Contract']:

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByContract'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['LOC', 'Location']:

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByLocation'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['LYR', 'Layer']:

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByLayer'
            return copy.deepcopy(pd.read_sql(script, self.connection))

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

    def _groupAnalysisPerils(self, AnalysisSID, perilsTemp):

        perilsAnalysis = self._getPerilsAnalysis(AnalysisSID)

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
            if not perils_exclusive == []:
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
            else:
                try:
                    script = 'Select PerilSetCode FROM AIRReference.dbo.tPeril WHERE PerilCode IN '+ str(tuple(perilsTemplateList))
                except:
                    script = 'Select PerilSetCode FROM AIRReference.dbo.tPeril WHERE PerilCode = '+ "'" + str(perilsTemplateList[0]) + "'"

                self.cursor.execute(script)
                info = copy.deepcopy(self.cursor.fetchall())
                perils = [info[i][0] for  i in range(len(info))]

            perilsAnalysisGrouped.append(perils)

        return perilsAnalysisGrouped

    def _getProgramID(self, AnalysisSID):

        script = 'SELECT ProgramSID FROM AIRProject.dbo.tLossAnalysisOption ' \
                 'WHERE AnalysisSID = ' + str(AnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        programID = info[0][0]

        return programID

    def _getProgramInfo(self, program_id):

        script = 'SELECT * FROM AIRReinsurance.dbo.tReinsurance '  \
                 'WHERE ProgramSID = ' + str(program_id)

        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        occ_ret = []
        occ_limit = []
        agg_ret = []
        agg_limit = []
        placed_percent = []
        ins_coins = []
        sequence_number = []
        for i in range(len(info)):

            occ_ret.append(info[i][17])
            occ_limit.append(info[i][16])
            agg_ret.append(info[i][19])
            agg_limit.append(info[i][18])
            placed_percent.append(info[i][21])
            ins_coins.append(info[i][26])
            sequence_number.append(info[i][40])

        return [occ_limit, occ_ret, agg_limit, agg_ret, placed_percent, ins_coins, sequence_number]