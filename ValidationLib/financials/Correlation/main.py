# Import internal packages
from ValidationLib.database.main import *
# Import external Python libraries
import pandas as pd


class Correlation:

    def __init__(self, server):

        setup = Database(server)
        self.connection = setup.connection
        self.cursor = setup.cursor

    def _get_correlation_factor(self, analysis_sid):

        if analysis_sid > 0:
            sql_script = 'SELECT * FROM AIRProject.dbo.tLossAnalysisOption WHERE AnalysisSID = {0}'.format(
                str(analysis_sid))
            self.cursor.execute(sql_script)
            info_analysis_option = copy.deepcopy(self.cursor.fetchall())

            intra_correlation = info_analysis_option[0][44]
            inter_correlation = info_analysis_option[0][45]

            if (intra_correlation and inter_correlation) >= 0.0:
                return intra_correlation, inter_correlation
            else:
                raise Exception('Invalid Intra or Inter Correlation Factor')

        else:
            raise Exception('Analysis SID should be greater than or equal to zero')

    def _get_sd(self, contract_result_sid, result_db, sd_type, tolerance, intra_correlation=None,
                location_result_sid=None,
                inter_correlation=None):

        result_df_detailed = ''
        result_df_summary = ''

        if sd_type == 'Inter':

            '''

            1. Using the  D.W. equation, compute the SD for each CatalogType, Each Model ID
            and for each event sd_type. Store the results in a temp table "Temp_Table_Inter".

            2. Read the temp table and sum the SD for Gu and Gr. Compare the values with DB values
            and generate the report.

            '''
            inter_correlation = inter_correlation

            # Extracting the SD from LossByContract table
            sql_script = 'USE ' + str(result_db) + \
                         '\nIF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables WHERE Table_Name = ' + "'" + str(
                'Temp_Table_Inter') + "')" + \
                         '\nDROP TABLE Temp_Table_Inter ' \
                         '\nSELECT con.CatalogTypeCode, ' \
                         'con.EventID, ' \
                         'con.ModelCode, ' \
                         'con.PerilSetCode, ' \
                         + str(inter_correlation) + '*SUM(con.GroundUpSD) + ' + str(
                1 - inter_correlation) + '*SQRT(SUM(POWER(con.[GroundUpSD],2))) AS CalculatedPortfolioGroundUpSD, ' \
                         + str(inter_correlation) + '*SUM(con.GrossSD) + ' + str(
                1 - inter_correlation) + '*SQRT(SUM(POWER(con.[GrossSD],2))) AS CalculatedPortfolioGrossSD, ' \
                                         'port.GroundUpSD as PortGuSD, ' \
                                         'port.GrossSD as PortGrSD ' \
                                         '\nINTO Temp_Table_Inter ' \
                                         'FROM [' + result_db + '].dbo.t' + str(
                contract_result_sid) + '_LOSS_ByContract con' \
                                       '\nJOIN [' + result_db + '].dbo.t' + str(
                contract_result_sid) + '_LOSS_ByEvent port ' \
                                       '\nON con.EventID = port.EventID AND con.ModelCode = ' \
                                       'port.ModelCode AND con.CatalogTypeCode = port.CatalogTypeCode ' \
                                       'AND con.PerilSetCode = port.PerilSetCode' \
                                       '\nGROUP BY con.CatalogTypeCode, con.EventID, con.ModelCode, ' \
                                       'con.PerilSetCode, port.GroundUpSD, port.GrossSD'

            self.cursor.execute(sql_script)
            self.connection.commit()
            script = 'SELECT * FROM [' + result_db + '].dbo.Temp_Table_Inter'
            result_df_detailed = pd.read_sql(script, self.connection)

            script = 'SELECT CatalogTypeCode, ModelCode, ' \
                     'SQRT(SUM(POWER(CalculatedPortfolioGroundUpSD,2))) AS CalculatedPortGuSD , SQRT(SUM(POWER(PortGuSD,2))) AS PortGuSD,' \
                     'SQRT(SUM(POWER(CalculatedPortfolioGrossSD,2))) AS CalculatedPortGrSD , SQRT(SUM(POWER(PortGrSD,2))) AS PortGrSD' \
                     '\nFROM [' + result_db + '].dbo.Temp_Table_Inter' \
                                              '\nGROUP BY CatalogTypeCode, ModelCode, PerilSetCode' \
                                              '\nORDER BY ModelCode'

            result_df_summary = pd.read_sql(script, self.connection)
            result_df_summary['Status'] = ''

            result_df_summary['DifferencePortGuSD_Percent'] = (result_df_summary['CalculatedPortGuSD'] - result_df_summary[
                'PortGuSD']) / result_df_summary['PortGuSD']
            result_df_summary['DifferencePortGrSD_Percent'] = (result_df_summary['CalculatedPortGrSD'] - result_df_summary[
                'PortGrSD']) / result_df_summary['PortGrSD']
            result_df_summary = result_df_summary.fillna(0)
            result_df_summary['DifferencePortGuSD_Percent'] = result_df_summary['DifferencePortGuSD_Percent'].astype(int)
            result_df_summary['DifferencePortGrSD_Percent'] = result_df_summary['DifferencePortGrSD_Percent'].astype(int)

            result_df_summary.loc[(abs(result_df_summary['DifferencePortGuSD_Percent']) >= (float(tolerance) / 100)) | (
                abs(result_df_summary['DifferencePortGrSD_Percent']) >= (float(tolerance) / 100)), 'Status'] = 'Fail'
            result_df_summary.loc[result_df_summary['Status'] == '', 'Status'] = 'Pass'

        elif sd_type == 'Intra':

            location_result_sid = location_result_sid
            intra_correlation = intra_correlation

            sql_script = 'USE ' + str(result_db) + \
                         '\nIF EXISTS (SELECT TABLE_NAME FROM INFORMATION_SCHEMA.Tables WHERE Table_Name = ' + "'" + str(
                'Temp_Table_Intra') + "')" + \
                         '\nDROP TABLE Temp_Table_Intra ' \
                         '\nSELECT loc.CatalogTypeCode, ' \
                         'loc.EventID, ' \
                         'loc.ModelCode, ' \
                         'loc.PerilSetCode, ' \
                         'dimLoc.ContractSID, ' \
                         + str(intra_correlation) + '*SUM(loc.GroundUpSD) + ' + str(
                1 - intra_correlation) + '*SQRT(SUM(POWER(loc.[GroundUpSD],2))) AS CalculatedConGroundUpSD, ' \
                         + str(intra_correlation) + '*SUM(loc.GrossSD) + ' + str(
                1 - intra_correlation) + '*SQRT(SUM(POWER(loc.[GrossSD],2))) AS CalculatedConGrossSD, ' \
                                         'con.GroundUpSD as ContractGuSD,' \
                                         'con.GrossSD as ContractGrSD ' \
                                         '\nINTO Temp_Table_Intra ' \
                                         'FROM [' + result_db + '].dbo.t' + str(
                location_result_sid) + '_LOSS_ByLocation loc' \
                                       '\nINNER JOIN [' + result_db + '].dbo.t' + str(
                location_result_sid) + '_LOSS_DimLocation dimLoc ON loc.LocationSID = dimLoc.LocationSID' \
                                       '\nJOIN [' + result_db + '].dbo.t' + str(
                contract_result_sid) + '_LOSS_ByContract con ' \
                                       '\nON dimLoc.ContractSID = con.ContractSID AND loc.EventID = con.EventID AND ' \
                                       'loc.ModelCode = con.ModelCode AND loc.CatalogTypeCode = con.CatalogTypeCode ' \
                                       'AND loc.PerilSetCode = con.PerilSetCode' \
                                       '\nGROUP BY loc.CatalogTypeCode, loc.EventID, loc.ModelCode, loc.PerilSetCode, ' \
                                       'dimLoc.ContractSID, con.GroundUpSD, con.GrossSD'

            self.cursor.execute(sql_script)
            self.connection.commit()
            script = 'SELECT * FROM [' + result_db + '].dbo.Temp_Table_Intra'
            result_df_detailed = pd.read_sql(script, self.connection)

            script = 'SELECT intra.CatalogTypeCode, intra.ModelCode, intra.PerilSetCode, dimCon.ContractID, ' \
                     'SQRT(SUM(POWER(intra.CalculatedConGroundUpSD,2))) AS CalculatedConGuSD , ' \
                     'SQRT(SUM(POWER(intra.ContractGuSD,2))) AS ContractGuSD,' \
                     'SQRT(SUM(POWER(intra.CalculatedConGrossSD,2))) AS CalculatedConGrSD , ' \
                     'SQRT(SUM(POWER(intra.ContractGrSD,2))) AS ContractGrSD' \
                     '\nFROM [' + result_db + '].dbo.Temp_Table_Intra intra' \
                                              '\n INNER JOIN [' + result_db + '].dbo.t' + str(
                location_result_sid) + '_LOSS_DimContract dimCon ON intra.ContractSID = dimCon.ContractSID' \
                                       '\nGROUP BY intra.CatalogTypeCode, intra.ModelCode, ' \
                                       'intra.PerilSetCode, dimCon.ContractID' \
                                       '\nORDER BY intra.ModelCode'

            result_df_summary = pd.read_sql(script, self.connection)

            result_df_summary['DifferenceConGuSD_Percent'] = (result_df_summary['CalculatedConGuSD'] - result_df_summary[
                'ContractGuSD']) / result_df_summary['ContractGuSD']
            result_df_summary['DifferenceConGrSD_Percent'] = (result_df_summary['CalculatedConGrSD'] - result_df_summary[
                'ContractGrSD']) / result_df_summary['ContractGrSD']
            result_df_summary = result_df_summary.fillna(0)
            result_df_summary['DifferenceConGuSD_Percent'] = result_df_summary['DifferenceConGuSD_Percent'].astype(int)
            result_df_summary['DifferenceConGrSD_Percent'] = result_df_summary['DifferenceConGrSD_Percent'].astype(int)

            result_df_summary['Status'] = ''
            result_df_summary.loc[(abs(result_df_summary['DifferenceConGuSD_Percent']) >= (float(tolerance) / 100)) | (
                abs(result_df_summary['DifferenceConGrSD_Percent']) >= (float(tolerance) / 100)), 'Status'] = 'Fail'
            result_df_summary.loc[result_df_summary['Status'] == '', 'Status'] = 'Pass'

        return result_df_detailed, result_df_summary
