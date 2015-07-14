__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# Import standard Python packages
import copy
# Import internal packages
from DbConn.main import *

# Import external Python libraries
import pyodbc
import pandas as pd



class CorrValidation:

    def __init__(self, server):

        setup = dbConnection(server)
        self.connection = setup.connection
        self.cursor = setup.cursor

    def _getCorrelationFactor(self, analysisSID):

        sql_script = 'SELECT * FROM AIRProject.dbo.tLossAnalysisOption ' \
                     'WHERE AnalysisSID = ' + str(analysisSID)
        self.cursor.execute(sql_script)
        info_analysis_option = copy.deepcopy(self.cursor.fetchall())

        intra_correlation = info_analysis_option[0][44]
        inter_correlation = info_analysis_option[0][45]

        return intra_correlation, inter_correlation

    def _getSD(self, contractResultSID, resultDB, type, **args):


        if type == 'Inter':

            interCorrelation = args.get('interCorrelation')

            # Extracting the SD from LossByContract table
            '''
            1. Using the  D.W. equation, compute the SD for each CatalogType, Each Model ID and for each event type.
            Store the results in a temp table "Temp_Table_Inter".

            2. Read the temp table and sum the SD for Gu and Gr. Compare the values with DB values and generate the report.

            '''
            sql_script =     'USE ' + str(resultDB) + \
                                '\nIF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables WHERE Table_Name = ' + "'" + str('Temp_Table_Inter') + "')" + \
                                '\nDROP TABLE Temp_Table_Inter ' \
                                '\nSELECT con.CatalogTypeCode, ' \
                                          'con.EventID, ' \
                                          'con.ModelCode, ' \
                                          'con.PerilSetCode, ' \
                                          + str(interCorrelation) + '*SUM(con.GroundUpSD) + ' + str(1-interCorrelation) + '*SQRT(SUM(POWER(con.[GroundUpSD],2))) AS CalculatedPortfolioGroundUpSD, ' \
                                          + str(interCorrelation) + '*SUM(con.GrossSD) + ' + str(1-interCorrelation) + '*SQRT(SUM(POWER(con.[GrossSD],2))) AS CalculatedPortfolioGrossSD, ' \
                                          'port.GroundUpSD as PortGuSD, ' \
                                          'port.GrossSD as PortGrSD ' \
                                          '\nINTO Temp_Table_Inter ' \
                                'FROM [' + resultDB + '].dbo.t' + str(contractResultSID) + '_LOSS_ByContract con' \
                                '\nJOIN [' + resultDB + '].dbo.t' + str(contractResultSID) + '_LOSS_ByEvent port ' \
                                '\nON con.EventID = port.EventID AND con.ModelCode = port.ModelCode AND con.CatalogTypeCode = port.CatalogTypeCode AND con.PerilSetCode = port.PerilSetCode' \
                                '\nGROUP BY con.CatalogTypeCode, con.EventID, con.ModelCode, con.PerilSetCode, port.GroundUpSD, port.GrossSD'


            self.cursor.execute(sql_script)
            self.connection.commit()
            script = 'SELECT * FROM [' + resultDB + '].dbo.Temp_Table_Inter'
            resultDF_detailed = pd.read_sql(script, self.connection)

            script =    'SELECT CatalogTypeCode, ModelCode, ' \
                        'SQRT(SUM(POWER(CalculatedPortfolioGroundUpSD,2))) AS CalculatedPortGuSD , SQRT(SUM(POWER(PortGuSD,2))) AS PortGuSD,' \
                        'SQRT(SUM(POWER(CalculatedPortfolioGroundUpSD,2))) - SQRT(SUM(POWER(PortGuSD,2))) AS DifferencePortGuSD, ' \
                        'SQRT(SUM(POWER(CalculatedPortfolioGrossSD,2))) AS CalculatedPortGrSD , SQRT(SUM(POWER(PortGrSD,2))) AS PortGrSD,' \
                        'SQRT(SUM(POWER(CalculatedPortfolioGrossSD,2))) - SQRT(SUM(POWER(PortGrSD,2))) AS DifferencePortGrSD' \
                        '\nFROM [' + resultDB + '].dbo.Temp_Table_Inter' \
                                                 '\nGROUP BY CatalogTypeCode, ModelCode, PerilSetCode' \
                                                 '\nORDER BY ModelCode'

            resultDF_summary = pd.read_sql(script, self.connection)
            resultDF_summary['Status'] = ''
            resultDF_summary.loc[(abs(resultDF_summary['DifferencePortGuSD'])>=0.01) | (abs(resultDF_summary['DifferencePortGrSD'])>=0.01), 'Status'] = 'Fail'
            resultDF_summary.loc[resultDF_summary['Status']=='', 'Status'] = 'Pass'

        elif type == 'Intra':

            locationResultSID = args.get('locationResultSID')
            intraCorrelation = args.get('intraCorrelation')

            sql_script =    'USE ' + str(resultDB) + \
                            '\nIF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables WHERE Table_Name = ' + "'" + str('Temp_Table_Intra') + "')" + \
                            '\nDROP TABLE Temp_Table_Intra ' \
                            '\nSELECT loc.CatalogTypeCode, ' \
                                      'loc.EventID, ' \
                                      'loc.ModelCode, ' \
                                      'loc.PerilSetCode, ' \
                                      'dimLoc.ContractSID, ' \
                                      + str(intraCorrelation) + '*SUM(loc.GroundUpSD) + ' + str(1-intraCorrelation) + '*SQRT(SUM(POWER(loc.[GroundUpSD],2))) AS CalculatedConGroundUpSD, ' \
                                      + str(intraCorrelation) + '*SUM(loc.GrossSD) + ' + str(1-intraCorrelation) + '*SQRT(SUM(POWER(loc.[GrossSD],2))) AS CalculatedConGrossSD, ' \
                                      'con.GroundUpSD as ContractGuSD,' \
                                      'con.GrossSD as ContractGrSD ' \
                                      '\nINTO Temp_Table_Intra ' \
                            'FROM [' + resultDB + '].dbo.t' + str(locationResultSID) + '_LOSS_ByLocation loc' \
                            '\nINNER JOIN [' + resultDB + '].dbo.t' + str(locationResultSID) + '_LOSS_DimLocation dimLoc ON loc.LocationSID = dimLoc.LocationSID' \
                            '\nJOIN [' + resultDB + '].dbo.t' + str(contractResultSID) + '_LOSS_ByContract con ' \
                            '\nON dimLoc.ContractSID = con.ContractSID AND loc.EventID = con.EventID AND loc.ModelCode = con.ModelCode AND loc.CatalogTypeCode = con.CatalogTypeCode AND loc.PerilSetCode = con.PerilSetCode' \
                            '\nGROUP BY loc.CatalogTypeCode, loc.EventID, loc.ModelCode, loc.PerilSetCode, dimLoc.ContractSID, con.GroundUpSD, con.GrossSD'

            self.cursor.execute(sql_script)
            self.connection.commit()

            script = 'SELECT * FROM [' + resultDB + '].dbo.Temp_Table_Intra'
            resultDF_detailed = pd.read_sql(script, self.connection)

            script =    'SELECT CatalogTypeCode, ModelCode, PerilSetCode, ContractSID, ' \
                        'SQRT(SUM(POWER(CalculatedConGroundUpSD,2))) AS CalculatedConGuSD , SQRT(SUM(POWER(ContractGuSD,2))) AS ContractGuSD,' \
                        'SQRT(SUM(POWER(CalculatedConGroundUpSD,2))) - SQRT(SUM(POWER(ContractGuSD,2))) AS DifferenceConGuSD,' \
                        'SQRT(SUM(POWER(CalculatedConGrossSD,2))) AS CalculatedConGrSD , SQRT(SUM(POWER(ContractGrSD,2))) AS ContractGrSD,' \
                        'SQRT(SUM(POWER(CalculatedConGrossSD,2))) - SQRT(SUM(POWER(ContractGrSD,2))) AS DifferenceConGrSD' \
                        '\nFROM [' + resultDB + '].dbo.Temp_Table_Intra' \
                                                 '\nGROUP BY CatalogTypeCode, ModelCode, PerilSetCode, ContractSID' \
                                                 '\nORDER BY ModelCode'

            resultDF_summary = pd.read_sql(script, self.connection)
            resultDF_summary['Status'] = ''
            resultDF_summary.loc[(abs(resultDF_summary['DifferenceConGuSD'])>=0.01) | (abs(resultDF_summary['DifferenceConGrSD'])>=0.01) , 'Status'] = 'Fail'
            resultDF_summary.loc[resultDF_summary['Status']=='', 'Status'] = 'Pass'

        return resultDF_detailed, resultDF_summary



