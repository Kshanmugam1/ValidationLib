__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# Import internal packages
import multiprocessing as mp

from ValidationLib.database.main import *


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    n = max(1, n)
    return [l[i:i + n] for i in range(0, len(l), n)]


def recovery_catxol(tuple, lossDF, programInfo):

    agg_limit_temp = copy.deepcopy(programInfo[2])
    agg_ret_temp = copy.deepcopy(programInfo[3])
    sample_lossDF = lossDF.loc[(lossDF['CatalogTypeCode'] == tuple[0]) & (lossDF['ModelCode'] == tuple[1]) &
                                       (lossDF['YearID'] == tuple[2])][['CatalogTypeCode', 'ModelCode',
                                                                 'YearID', 'EventID', 'NetOfPreCATLoss',
                                                                 'PostCATNetLoss']].reset_index().drop('index', axis=1)
    sample_lossDF['Recovery'] = sample_lossDF['NetOfPreCATLoss'] - programInfo[1]

    sample_lossDF.loc[sample_lossDF['Recovery']<0, 'Recovery'] = 0
    sample_lossDF.loc[sample_lossDF['Recovery']>programInfo[0], 'Recovery'] = programInfo[0]

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

    sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * (1 - (programInfo[5]))
    sample_lossDF['Recovery'] = sample_lossDF['Recovery'] * programInfo[4]

    return sample_lossDF['Recovery'].values


def recovery_quota_share(tuple, lossDF, programInfo):

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


def apply_terms_multithread(location_information, i):
    if location_information['LimitTypeCode'][i] == 'S':

        # Deductible type: "S"
        if location_information['DeductibleTypeCode'][i] == 'S':
            coverage = ['A', 'B', 'C', 'D']
            ded = []
            limit = []
            for j in range(len(coverage)):

                if location_information['Deductible' + str(j + 1)][i] > 1:
                    ded.append(location_information['Deductible' + str(j + 1)][i])
                else:
                    ded.append(
                        location_information['Deductible' + str(j + 1)][i] * location_information['Limit' + str(j + 1)][
                            i])
                limit.append(location_information['Limit' + str(j + 1)][i])

            ded = sum(ded)
            limit = sum(limit)
            if location_information['AIROccupancyCode'][i] == 311:
                return min(max((location_information['exposedGroundUp'][i] - ded), 0), limit)

        # Deductible type: "C"
        elif location_information['DeductibleTypeCode'][i] == 'C':
            coverage = ['A', 'B', 'C', 'D']
            total_gross = []
            for j in range(len(coverage)):
                if location_information['ReplacementValue' + coverage[j]][i] > 0.0:
                    if location_information['Deductible' + str(j + 1)][i] > 1:
                        ded = location_information['Deductible' + str(j + 1)][i]
                    else:
                        ded = location_information['Deductible' + str(j + 1)][i] * \
                              location_information['Limit' + str(j + 1)][i]
                    limit = location_information['Limit' + str(j + 1)][i]  # * location_information['DamageRatio'][i]
                    # Add the condition for Residential Occupancy
                    if location_information['AIROccupancyCode'][i] == 311:
                        temp_gross = max((location_information['ReplacementValue' + coverage[j]][i] *
                                          location_information['DamageRatio'][i]) - ded, 0)
                        gross = min(temp_gross, limit)
                else:
                    gross = 0.0
                total_gross.append(gross)
            return sum(total_gross)

        # Deductible type: "CB"
        elif location_information['DeductibleTypeCode'][i] == 'CB':
            coverage = ['A', 'B', 'C']
            ded = []
            limit = []
            for j in range(len(coverage)):
                if location_information['Deductible' + str(j + 1)][i] > 1:
                    ded.append(location_information['Deductible' + str(j + 1)][i])
                else:
                    ded.append(
                        location_information['Deductible' + str(j + 1)][i] * location_information['Limit' + str(j + 1)][
                            i])
                limit.append(location_information['Limit' + str(j + 1)][i])
            ded = sum(ded)
            limit = sum(limit)
            # Add the condition for Residential Occupancy
            if location_information['AIROccupancyCode'][i] == 311:
                return min(max(((location_information['exposedGroundUp'][i] -
                                 location_information['ReplacementValueD'][i] *
                                 location_information['DamageRatio'][i]) - ded), 0), limit) + \
                       location_information['ReplacementValueD'][i] * location_information['DamageRatio'][i]

        # Deductible type: "CT"
        elif location_information['DeductibleTypeCode'][i] == 'CT':
            coverage = ['A', 'B', 'C']
            ded = []
            limit = []
            for j in range(len(coverage)):
                if location_information['Deductible' + str(j + 1)][i] > 1:
                    ded.append(location_information['Deductible' + str(j + 1)][i])
                else:
                    ded.append(location_information['Deductible' + str(j + 1)][i] *
                               location_information['Limit' + str(j + 1)][i])
                limit.append(location_information['Limit' + str(j + 1)][i])  # * location_information['DamageRatio'][i]

            ded = sum(ded)
            limit = sum(limit)
            if location_information['AIROccupancyCode'][i] == 311:
                gross_bldg = \
                    min(max(((location_information['exposedGroundUp'][i] -
                              location_information['ReplacementValueD'][i] *
                              location_information['DamageRatio'][i]) - ded), 0), limit)
                if location_information['Deductible4'][i] > 1:
                    gross_time = min(max(((location_information['ReplacementValueD'][i] *
                                           location_information['DamageRatio'][i]) -
                                          location_information['Deductible4'][i]), 0),
                                     location_information['Limit4'][i])
                else:
                    gross_time = \
                        min(max(((location_information['ReplacementValueD'][i] *
                                  location_information['DamageRatio'][i]) -
                                 (location_information['Deductible4'][i] * location_information['Limit4'][i])), 0),
                            location_information['Limit4'][i])
                return gross_bldg + gross_time

        # Deductible type: "PL"
        elif location_information['DeductibleTypeCode'][i] == 'PL':
            coverage = ['A']
            ded = []
            limit = []
            for j in range(len(coverage)):
                if location_information['Deductible' + str(j + 1)][i] > 1:
                    pass
                else:
                    ded.append(location_information['Deductible' + str(j + 1)][i] *
                               ((location_information['ReplacementValueA'][i] +
                                 location_information['ReplacementValueB'][i] +
                                 location_information['ReplacementValueC'][i] +
                                 location_information['ReplacementValueD'][i]) * location_information['DamageRatio'][
                                    i]))

            ded = sum(ded)
            limit = location_information['Limit1'][i] + \
                    location_information['Limit2'][i] + \
                    location_information['Limit3'][i] + \
                    location_information['Limit4'][i]
            # Add the condition for Residential Occupancy
            if location_information['AIROccupancyCode'][i] == 311:
                return min(max((location_information['exposedGroundUp'][i] - ded), 0), limit)

        # Deductible type: "ML"
        elif location_information['DeductibleTypeCode'][i] == 'ML':

            if location_information['Deductible2'][i] > 1 or location_information['Deductible1'][i] < 1 or \
                            location_information['Deductible4'][i] < 1:
                pass
            else:
                ground_up_loss_1 = ((location_information['ReplacementValueA'][i] +
                                     location_information['ReplacementValueB'][i] +
                                     location_information['ReplacementValueC'][i]) *
                                    location_information['DamageRatio'][i])

                limit = location_information['Limit1'][i] + \
                        location_information['Limit2'][i] + \
                        location_information['Limit3'][i] + \
                        location_information['Limit4'][i]

                ded1 = location_information['Deductible2'][i] * ground_up_loss_1

                ded2 = location_information['Deductible1'][i]

                ded3 = location_information['Deductible4'][i]
                # Add the condition for Residential Occupancy
                if location_information['AIROccupancyCode'][i] == 311:
                    if ded2 >= ded1:
                        gross_1 = max(location_information['exposedGroundUp'][i] - ded2, 0)
                    else:
                        gross_1 = max(location_information['exposedGroundUp'][i] - ded1, 0)
                    return min(max(gross_1 - ded3, 0), limit)

        # deductible type: "MP"
        elif location_information['DeductibleTypeCode'][i] == 'MP':

            if location_information['Deductible1'][i] > 1:
                ded = location_information['Deductible1'][i]
            else:
                ded = location_information['Deductible1'][i] * location_information['Limit1'][i]

            limit = location_information['Limit1'][i] + \
                    location_information['Limit2'][i] + \
                    location_information['Limit3'][i] + \
                    location_information['Limit4'][i]
            # Add the condition for Residential Occupancy
            if location_information['AIROccupancyCode'][i] == 311:
                if location_information['Deductible1'][i] < location_information['ReplacementValueA'][i] * \
                        location_information['DamageRatio'][i]:
                    ded1 = max(location_information['ReplacementValueA'][i] * location_information['DamageRatio'][i] -
                               ded, 0)
                else:
                    ded1 = max((location_information['ReplacementValueA'][i] +
                                location_information['ReplacementValueB'][i]) * location_information['DamageRatio'][i] -
                               ded, 0)
                return min(ded1 + \
                           (location_information['ReplacementValueB'][i] +
                            location_information['ReplacementValueC'][i] +
                            location_information['ReplacementValueD'][i]) *
                           location_information['DamageRatio'][i], limit)

        # Deductible type: "FR"
        elif location_information['DeductibleTypeCode'][i] == 'FR':
            coverage = ['A', 'B', 'C', 'D']
            total_gross = []
            if location_information['Deductible1'][i] < 1:
                pass
            else:
                # Add the condition for Residential Occupancy
                if location_information['AIROccupancyCode'][i] == 311:
                    for j in range(len(coverage)):
                        if location_information['ReplacementValue' + coverage[j]][i] * \
                                location_information['DamageRatio'][i] > \
                                location_information['Deductible' + str(j + 1)][i]:
                            total_gross.append(location_information['ReplacementValue' + coverage[j]][i] *
                                               location_information['DamageRatio'][i])
                        else:
                            total_gross.append(0.0)
                    return sum(total_gross)


class Catxol:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.server = server
        self.setup = Database(server)
        self.connection = self.setup.connection
        self.cursor = self.setup.cursor

    def get_tasks(self, resultDB, resultSID):

        lossDF = self.setup.loss_df(resultDB, resultSID, 'PORT')
        lossDF.sort(['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID'], inplace=True)

        lossDF = lossDF[['CatalogTypeCode', 'ModelCode', 'YearID', 'EventID', 'NetOfPreCATLoss', 'PostCATNetLoss']]

        Catalogtypes = lossDF.loc[:, 'CatalogTypeCode'].unique()
        task_list = []
        for c in Catalogtypes:
            ModelCode = lossDF.loc[lossDF['CatalogTypeCode'] == c, 'ModelCode'].unique()
            for m in ModelCode:
                yearID = lossDF.loc[(lossDF['CatalogTypeCode'] == c) & (lossDF['ModelCode'] == m) , 'YearID'].unique()
                for y in yearID:
                    task_list.append(tuple([c,m,y]))

        return task_list, lossDF

    def validate(self, result):


        result.insert(0, 'Status', '-')

        result['DifferencePercent'] = (result['CalculatedPostCATNetLoss'] - result['PostCATNetLoss'])/result['PostCATNetLoss']
        result = result.fillna(0)
        result['DifferencePercent'] = result['DifferencePercent'].astype(int)
        result.loc[abs(result['DifferencePercent']) < 0.1, 'Status'] = 'Pass'
        result.loc[result['Status'] == '-', 'Status'] = 'Fail'

        return result


class Correlation:

    def __init__(self, server):

        setup = Database(server)
        self.connection = setup.connection
        self.cursor = setup.cursor

    def correlation_factor(self, analysis_sid):

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

    def loss_sd(self, contract_result_sid, result_db, sd_type, tolerance, intra_correlation=None,
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


class QSValidation:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.server = server
        self.setup = Database(server)
        self.connection = self.setup.connection
        self.cursor = self.setup.cursor

    def get_tasks(self, resultDB, resultSID):

        lossDF = self.setup.loss_df(resultDB, resultSID, 'PORT')
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


class FinancialTerms:
    def apply_terms(self, location_information):

        def get_result(i):
            return [i, results[i].get()]

        location_information['expectedGross'] = ""
        pool = mp.Pool()
        results = [pool.apply_async(apply_terms_multithread, args=(location_information, i))
                   for i in range(len(location_information))]
        output = [results[i].get() for i in range(len(results))]
        location_information['expectedGross'] = output
        location_information['Status'] = 'Fail'
        for i in range(len(location_information)):
            if (abs(location_information['exposedGross'][i] - location_information['expectedGross'][i]) <= 1):
                location_information.loc[i, 'Status'] = 'Pass'

        return location_information
