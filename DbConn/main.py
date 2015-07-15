__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

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

        if type == ('EA' or 'LOB'):

            script = 'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByExposureAttribute'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type == ('CONSUM' or 'Contract Summary'):

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByContractSummary'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type == ('LOCSUM' or 'Location Summary'):

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByLocationSummary'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type == ('PORT' or 'Event'):

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type == ('CON' or 'Contract'):

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByContract'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type == ('LOC' or 'Location'):

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByLocation'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type == ('LYR' or 'Layer'):

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByLayer'
            return copy.deepcopy(pd.read_sql(script, self.connection))









