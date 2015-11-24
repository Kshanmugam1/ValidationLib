# Import standard Python packages
import copy

import pip


# Import external Python libraries
try:
    import pandas as pd
except:
    pip.main(['install', 'pandas'])
    import pandas as pd
try:
    import pyodbc
except:
    pip.main(['install', 'pyodbc'])
    import pyodbc


class Database:

    def __init__(self, server):

        # Initializing the connection and cursor
        self.connection = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + '; UID=airadmin; PWD=Air$admin123')
        self.cursor = self.connection.cursor()

    def analysis_sid(self, analysisName):

        script = 'SELECT AnalysisSID from AIRProject.dbo.tAnalysis WHERE AnalysisName = ' + "'" + str(analysisName) + "'"
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        return info[-1][0]

    def result_sid(self, AnalysisSID):

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

    def mod_analysis_sid(self, ModAnalysisSID):

        script = 'SELECT BaseAnalysisSID FROM AIRProject.dbo.tLossAnalysisOption ' \
                 'WHERE AnalysisSID = ' + str(ModAnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        baseAnalysisSID = info[0][0]

        return baseAnalysisSID

    def loss_mod_temp_id(self, ModAnalysisSID):

        script = 'SELECT LossModTemplateSID FROM AIRProject.dbo.tLossAnalysisOption ' \
                 'WHERE AnalysisSID = ' + str(ModAnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        LossModTempID = info[0][0]

        return LossModTempID

    def perils_analysis(self, ModAnalysisSID):

        script = 'SELECT PerilSetCode FROM AIRProject.dbo.tLossAnalysisOption ' \
                 'WHERE AnalysisSID = ' + str(ModAnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        PerilsAnalysis = info[0][0]

        return PerilsAnalysis

    def analysis_info(self, AnalysisSID):

        script =  'SELECT * FROM AIRProject.dbo.tLossAnalysisOption ' \
                  'WHERE AnalysisSID = ' + str(AnalysisSID)
        self.cursor.execute(script)

        return (copy.deepcopy(self.cursor.fetchall()))

    def loss_df(self, resultDB, resultSID, type):

        if type in ['EA', 'LOB']:

            script = 'SELECT a.*, id.ExposureAttribute FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByExposureAttribute a ' \
                     'JOIN [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_DimExposureAttribute id ON ' \
                                                                        'a.ExposureAttributeSID = id.ExposureAttributeSID'
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

            script =  'SELECT a.*, id.ContractID FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByContract a ' \
                      'JOIN [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_DimContract id ON ' \
                                                                         'a.ContractSID = id.ContractSID'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['LOC', 'Location']:

            script =  'SELECT a.*, id.LocationID FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByLocation a' \
                      ' JOIN [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_DimLocation id ' \
                                                                          'ON a.LocationSID = id.LocationSID'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['LYR', 'Layer']:

            script =  'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByLayer'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['EP']:

            script = 'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_AnnualEP'
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if type in ['GEOL']:

            script = 'SELECT * FROM [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByGeo'
            return copy.deepcopy(pd.read_sql(script, self.connection))

    def loss_mod_info(self, LossModTempID):

        script = 'SELECT * FROM AIRUserSetting.dbo.tLossModTemplateRule ' \
                 'WHERE LossModTemplateSID = ' + str(LossModTempID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())

        coverage = []
        factor = []
        LOB = []
        admin_boundary = []
        admin_boundary_temp = []
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

        script = 'SELECT AdminBoundarySID FROM AIRUserSetting.dbo.tLossModTemplateRule ' \
                 'WHERE LossModTemplateSID = ' + str(LossModTempID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        if info:
            for i in range(len(info)):
                if not info[i][0] == None:
                    script = 'Select GeographySID from ' \
                             '[AIRUserSetting].[dbo].[tAdminBoundaryDetail] ' \
                             'Where [AdminBoundarySID] = ' + str(info[i][0])
                    self.cursor.execute(script)
                    info = copy.deepcopy(self.cursor.fetchall())
                    for i in range(len(info)):
                        admin_boundary_temp.append(info[i][0])
                else:
                    admin_boundary_temp = []
                admin_boundary.append(admin_boundary_temp)

        return perilsTemp, coverage, LOB, admin_boundary, occupancy, construction, yearBuilt, stories, contractID, locationID, factor

    def group_analysis_perils(self, AnalysisSID, perilsTemp):

        perilsAnalysis = self.perils_analysis(AnalysisSID)

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

    def program_id(self, AnalysisSID):

        script = 'SELECT ProgramSID FROM AIRProject.dbo.tLossAnalysisOption ' \
                 'WHERE AnalysisSID = ' + str(AnalysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        programID = info[0][0]

        return programID

    def program_info(self, program_id, type):

        script = 'SELECT * FROM AIRReinsurance.dbo.tReinsurance '  \
                 'WHERE ProgramSID = ' + str(program_id)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        occ_limit = []
        agg_limit = []
        placed_percent = []
        sequence_number = []

        if type == 'catxol':
            occ_ret = []
            agg_ret = []
            ins_coins = []

            for i in range(len(info)):

                occ_ret.append(info[i][17])
                occ_limit.append(info[i][16])
                agg_ret.append(info[i][19])
                agg_limit.append(info[i][18])
                placed_percent.append(info[i][21])
                ins_coins.append(info[i][26])
                sequence_number.append(info[i][40])

            return [occ_limit, occ_ret, agg_limit, agg_ret, placed_percent, ins_coins, sequence_number]
        elif type == 'qs':

            ceded_amount = []
            for i in range(len(info)):

                occ_limit.append(info[i][16])
                agg_limit.append(info[i][18])
                ceded_amount.append(info[i][20])
                placed_percent.append(info[i][21])
                sequence_number.append(info[i][40])

            return[occ_limit, agg_limit, ceded_amount, placed_percent, sequence_number]

    def event_loss_ep(self, resultDB, resultSID, financial_prsp, type):

        if financial_prsp == 'GU':

            if type == 'OCC':

                script = 'SELECT YearID, MAX(GroundUpLoss) as GU FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By GU DESC'

            elif type == 'AGG':

                script = 'SELECT YearID, SUM(GroundUpLoss) as GU FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By GU DESC'

        elif financial_prsp == 'GR':

            if type == 'OCC':

                script = 'SELECT YearID, MAX(GrossLoss) as GR FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By GR DESC'

            elif type == 'AGG':

                script = 'SELECT YearID, SUM(GrossLoss) as GR FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By GR DESC'

        elif financial_prsp == 'NT':

            if type == 'OCC':

                script = 'SELECT YearID, MAX(NetOfPreCATLoss) as NT FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By NT DESC'

            elif type == 'AGG':

                script = 'SELECT YearID, SUM(NetOfPreCATLoss) as NT FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By NT DESC'

        elif financial_prsp == 'POST':

            if type == 'OCC':

                script = 'SELECT YearID, MAX(PostCATNetLoss) as POST FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By POST DESC'

            elif type == 'AGG':

                script = 'SELECT YearID, SUM(PostCATNetLoss) as POST FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By POST DESC'

        elif financial_prsp == 'RT':

            if type == 'OCC':

                script = 'SELECT YearID, MAX(RetainedLoss) as RT FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By RT DESC'

            elif type == 'AGG':

                script = 'SELECT YearID, SUM(RetainedLoss) as RT FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByEvent WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID ORDER By RT DESC'
        return copy.deepcopy(pd.read_sql(script, self.connection))

    def rei_loss_ep(self, resultDB, resultSID, financial_prsp, type):

        if financial_prsp == 'GU':

            if type == 'OCC':

                script = 'SELECT ModelCode, YearID, PerilSetCode, MAX(GroundUpLoss) as GU FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByReinsurance WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID, ModelCode, PerilSetCode ORDER By GU DESC'

            elif type == 'AGG':

                script = 'SELECT ModelCode, YearID, PerilSetCode, SUM(GroundUpLoss) as GU FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByReinsurance WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID, ModelCode, PerilSetCode ORDER By GU DESC'

        elif financial_prsp == 'GR':

            if type == 'OCC':

                script = 'SELECT ModelCode, YearID, PerilSetCode, MAX(GrossLoss) as GR FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByReinsurance WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID, ModelCode, PerilSetCode ORDER By GR DESC'

            elif type == 'AGG':

                script = 'SELECT ModelCode, YearID, PerilSetCode, SUM(GrossLoss) as GR FROM ' \
                         '[' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_ByReinsurance WHERE CatalogTypeCode = ' + "'STC'" + \
                         ' Group By YearID, ModelCode, PerilSetCode ORDER By GR DESC'

        return copy.deepcopy(pd.read_sql(script, self.connection))

    def ep_points(self, analysisSID):

        script = 'SELECT EP1, EP2, EP3, EP4, EP5, EP6, EP7, EP8, EP9, EP10, EP11, EP12, EP13, ' \
                 'EP14, EP15 FROM [AIRProject].[dbo].[tLossAnalysisOption] ' \
                 'WHERE AnalysisSID = ' + str(analysisSID)
        self.cursor.execute(script)
        info = copy.deepcopy(self.cursor.fetchall())
        return info[0]

    def ep_summary(self, resultDB, resultSID):

        script = 'SELECT * from [' + resultDB + '].dbo.t' + str(resultSID) + '_LOSS_AnnualEPSummary'
        return copy.deepcopy(pd.read_sql(script, self.connection))

    def table_names(self, db, criteria=None):

        script = 'USE [' + str(db) + '] '
        self.cursor.execute(script)

        if not criteria is None:
            script = 'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME like ' \
                     + "'" + criteria + "'"
        else:
            script = 'SELECT * FROM INFORMATION_SCHEMA.TABLES'

        return copy.deepcopy(pd.read_sql(script, self.connection))

    def location_info_disagg(self, exp_db, exp_name):

        script = 'SELECT ' \
                 'b.GeographySID, ' \
                 'b.AIROccupancyCode, ' \
                 'b.LocationID, ' \
                 'g.ContractID, ' \
                 'b.CountryCode, ' \
                 'c.GeoLevelCode as GeoLevelCode, ' \
                 'e.ExchangeRate, ' \
                 'f.intWeightTypeId, ' \
                 'b.ReplacementValueA, ' \
                 'b.ReplacementValueB, ' \
                 'b.ReplacementValueC, ' \
                 'b.ReplacementValueD, ' \
                 'i.strTableSuffix, ' \
                 'b.AreaName, ' \
                 'b.CurrencyCode, ' \
                 'b.GeoCoderCode, ' \
                 'b.GeoMatchLevelCode, ' \
                 'b.Latitude, ' \
                 'b.Longitude ' \
                 'FROM (SELECT [ExposureSetSID], ' \
                 '[ExposureSetName], ' \
                 '[StatusCode], ' \
                 '[EnteredDate], ' \
                 '[EditedDate], ' \
                 '[Description], ' \
                 '[RowVersion] ' \
                 'FROM [' + str(exp_db) + '].[dbo].[tExposureSet] ' \
                                          'where ExposureSetName = '+ "'" + str(exp_name) + "'" + ') a ' \
                 'JOIN ['+str(exp_db) + '].[dbo].[tLocation] b ON a.ExposureSetSID = b.ExposureSetSID ' \
                 'JOIN [AIRGeography].[dbo].[tGeography] c on b.GeographySID = c.GeographySID ' \
                 'JOIN [AIRReference].[dbo].[tCountryCurrencyXref] d on c.CountryCode=d.CountryCode ' \
                 'JOIN [AIRUserSetting].[dbo].[tCurrencyExchangeRateSetConversion] e on ' \
                 'd.CurrencyCode=e.CurrencyCode ' \
                 'JOIN [AIRGeography].[dbo].[TblOccAirWeightType_xref] f ON b.AIROccupancyCode = f.intOccAir ' \
                 'JOIN ['+str(exp_db) + '].[dbo].[tContract] g ON b.ContractSID = g.ContractSID ' \
                 'JOIN [AIRGeography].[dbo].[TblAreaExternal] h ON c.GeographySID = h.GeographySID ' \
                 'FULL JOIN [AIRGeography].[dbo].[TblGridPartitionRef] i ON h.intLevel1 = i.intAreaLevel1 ' \
                 ' where  e.CurrencyExchangeRateSetSID=1 and d.IsDefault=1'

        return copy.deepcopy(pd.read_sql(script, self.connection))

    def staging_locations(self, db, location_table, contract_table, exp_db):

        script = 'SELECT ' \
                 'b.[ContractID], ' \
                 'a.[LocationID], ' \
                 'CONVERT(varchar(max),a.[guidLocation],2) as GuidLocation, ' \
                 'a.[Latitude], ' \
                 'a.[Longitude], ' \
                 'a.[ChildLocationCount], ' \
                 'a.[ReplacementValueA], ' \
                 'a.[ReplacementValueB], ' \
                 'a.[ReplacementValueC], ' \
                 'a.[ReplacementValueD], ' \
                 'a.[LocationTypeCode], ' \
                 'CONVERT(varchar(max),a.[guidLocationParent],2) as GuidLocationParent, ' \
                 'c.GeographySID ' \
                 'FROM ['+ str(db) + '].[dbo].['+ str(location_table)+ '] a ' \
                 'JOIN ['+ str(db) + '].[dbo].['+ str(contract_table)+ '] b ON a.guidContract = b.guidContract ' \
                 'JOIN ['+ str(exp_db) + '].[dbo].[tLocation] c ON a.LocationSID = c.LocationSID'
        return copy.deepcopy(pd.read_sql(script, self.connection))

    def disagg_rv(self, location_info):
        try:
            script = 'SELECT a.fltGeoLat as Latitude,' \
                     'a.fltGeoLong as Longitude, ' \
                     'a.fltWeight, ' \
                     'b.dblMinCovA, ' \
                     'b.dblMinCovB, ' \
                     'b.dblMinCovC, ' \
                     'b.dblMinCovD, ' \
                     + str(location_info[8]) + '*a.fltWeight as CalcReplacementValueA, ' \
                     + str(location_info[9]) + '*a.fltWeight as CalcReplacementValueB,' \
                     + str(location_info[10]) + '*a.fltWeight as CalcReplacementValueC, ' \
                     + str(location_info[11]) + '*a.fltWeight as CalcReplacementValueD, ' \
                     'c.ExchangeRate ' \
                     'From (SELECT [guidExternalSource],' \
                     '[fltGeoLat],' \
                     '[fltGeoLong],' \
                     '[intWeightTypeId],' \
                     '[fltWeight],' \
                     '[GridGeographySID] FROM [AIRGeography].[dbo].[TblSourceTargetMap' + location_info[12] + '] temp ' \
                     'JOIN [AIRGeography].[dbo].[tGeography] geo on temp.guidExternalSource = geo.GuidExternal ' \
                     'where geo.GeographySID =' + str(location_info[0]) + ' and temp.intWeightTypeId = ' + str(location_info[7]) + ') a ' \
                     'JOIN [AIRGeography].[dbo].[TblSourceDefaultTarget_Xref] b ' \
                                                                         'ON a.guidExternalSource = b.guidExternalSource ' \
                                                                         'and a.intWeightTypeId = b.intWeightTypeId ' \
                     'JOIN [AIRUserSetting].[dbo].[tCurrencyExchangeRateSetConversion] c on b.strCurrency = c.CurrencyCode ' \
                     'WHERE c.CurrencyExchangeRateSetSID=1 '

            return copy.deepcopy(pd.read_sql(script, self.connection))
        except:
            return pd.DataFrame()

    def required_disagg_geo(self, location_info):

        script = 'SELECT GeoLevelCode FROM [AIRGeography].[dbo].[tDisaggregatedGeoLevel] WHERE CountryCode = ' + "'" + str(location_info[4]) + "'"
        data = copy.deepcopy(pd.read_sql(script, self.connection))
        return data.iloc[:, 0].values.tolist()

    def analysis_option(self, analysis_sid):

        script = 'SELECT ' \
                 'a.AnalysisTypeCode, ' \
                 'a.SourceTemplateName, ' \
                 'd.EventSet, ' \
                 'c.PerilSet, ' \
                 'b.EventFilter, ' \
                 'b.DemandSurgeOptionCode, ' \
                 'b.EnableCorrelation, ' \
                 'b.ApplyDisaggregation, ' \
                 'b.AveragePropertyOptionCode, ' \
                 'b.RemapConstructionOccupancy, ' \
                 'b.LossModOption, ' \
                 'b.MoveMarineCraftGeocodesToCoast, ' \
                 'b.SaveGroundUp, b.SaveRetained, b.SavePreLayerGross, b.SaveGross, ' \
                 'b.SaveNetOfPreCAT, b.SavePostCATNet, ' \
                 'e.OutputType, ' \
                 'b.SaveCoverage, b.SaveClaims, b.SaveInjury, b.SaveMAOL, ' \
                 'b.SaveSummaryByPeril, b.SaveSummaryByModel, ' \
                 'b.BaseAnalysisSID, ' \
                 'c.CoversStormSurge, ' \
                 'c.CoversPrecipitationFlood, ' \
                 'c.CoversThunderstorm, ' \
                 'c.CoversInlandFlood,  ' \
                 'c.CoversTerrorism ' \
                 'FROM [AIRProject].[dbo].[tAnalysis] a ' \
                 'JOIN [AIRProject].[dbo].[tLossAnalysisOption] b on a.AnalysisSID = b.AnalysisSID ' \
                 'JOIN [AIRReference].[dbo].[tPerilSet] c on b.PerilSetCode = c.PerilSetCode ' \
                 'JOIN [AIRUserSetting].[dbo].[tEventSet] d on b.EventSetSID = d.EventSetSID ' \
                 'JOIN [AIRReference].[dbo].[tOutputType] e on b.OutputTypeCode = e.OutputTypeCode ' \
                 'WHERE a.AnalysisSID = ' + str(analysis_sid)

        return copy.deepcopy(pd.read_sql(script, self.connection))

    def get_analysis(self, analysis_sid):

        script = 'SELECT AnalysisTargetName ' \
                 'FROM [AIRProject].[dbo].[tAnalysisTargetXref] a ' \
                 'JOIN [AIRProject].[dbo].[tAnalysis] b on a.AnalysisSID = b.AnalysisSID ' \
                 'WHERE a.AnalysisSID = ' + str(analysis_sid)
        return copy.deepcopy(pd.read_sql(script, self.connection))

    def get_loc_info_policy(self, exposure_db, exposure_name, result_db, result_sid):

        script = 'SELECT ' \
                 'ContractID, LocationID, ' \
                 'AIROccupancyCode, ' \
                 'ReplacementValueA, ReplacementValueB, ReplacementValueC, ReplacementValueD, ' \
                 'f.DamageRatio/100.0 as DamageRatio, ' \
                 'LimitTypeCode, Limit1, Limit2, Limit3, Limit4, ' \
                 'DeductibleTypeCode, Deductible1, Deductible2, Deductible3, Deductible4, ' \
                 'Participation1, Participation2, ' \
                 'e.exposedGroundUp, e.exposedGross ' \
                 'FROM ' + exposure_db + '..tLocation a ' \
                                         'JOIN ' + exposure_db + '..tContract b ON a.ContractSID = b.ContractSID ' \
                                                                 'JOIN ' + exposure_db + '..tLocTerm c ON a.ContractSID = c.ContractSID and a.LocationSID = c.LocationSID ' \
                                                                                         'JOIN ' + exposure_db + '..tExposureSet d ON a.ExposureSetSID = d.ExposureSetSID  ' \
                                                                                                                 'JOIN ' + result_db + '..t' + str(
            result_sid) + '_EC_ByLocation e ON e.LocationSID = a.LocationSID ' \
                          'JOIN ' + result_db + '..t' + str(
            result_sid) + '_EC_DimSubAccumulator f on e.AccumulatorID = f.AccumulatorID ' \
                          'WHERE d.ExposureSetName = ' + "'" + exposure_name + "'"

        return copy.deepcopy(pd.read_sql(script, self.connection))

    def get_peril_option(self, analysis_sid, peril_option):

        if peril_option == 'SS':
            script = 'SELECT UserLineOfBusiness, Percentage FROM [AIRProject].[dbo].[tLossAnalysisPerilSetULOBOption] ' \
                     'WHERE PerilSetCode = 256 and AnalysisSID = ' + str(analysis_sid)
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if peril_option == 'PF':
            script = 'SELECT UserLineOfBusiness, Percentage FROM [AIRProject].[dbo].[tLossAnalysisPerilSetULOBOption] ' \
                     'WHERE PerilSetCode = 4096 and AnalysisSID = ' + str(analysis_sid)
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if peril_option == 'ST':
            script = 'SELECT STStormOptionCode FROM [AIRProject].[dbo].[tLossAnalysisSTStormOption] ' \
                     'WHERE AnalysisSID = ' + str(analysis_sid)
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if peril_option == 'IF':
            script = 'SELECT CustomFloodZoneCode FROM [AIRProject].[dbo].[tLossAnalysisFloodOption] ' \
                     'WHERE AnalysisSID = ' + str(analysis_sid)
            return copy.deepcopy(pd.read_sql(script, self.connection))

        if peril_option == 'TR':
            script = 'SELECT isnull(Description+' + "'" + '_' + "'" ',' + "'" + "'"') + TerrorismOption FROM [AIRProject].[dbo].[tLossAnalysisTerrorismOption] a ' \
                                                                                'JOIN [AIRReference].[dbo].[tTerrorismOption] b on a.TerrorismOptionCode = b.TerrorismOptionCode ' \
                     'where a.AnalysisSID = ' + str(analysis_sid)

            return copy.deepcopy(pd.read_sql(script, self.connection))

    def exp_location_information(self, exposure_db, exposure_name):

        script = 'SELECT ' \
                 'c.ContractID, LocationID, LocationName, LocationGroup, ' \
                 'IsPrimaryLocation, ISOBIN, ' \
                 'a.InceptionDate, a.ExpirationDate, ' \
                 'Address, City, CountryName, AreaName, SubareaName, Subarea2Name, ' \
                 'CRESTAName, PostalCode, GeoMatchLevelCode, Latitude, Longitude ' \
                 'FROM [SKExp].[dbo].[tLocation] a ' \
                 'JOIN [SKExp].[dbo].[tExposureSet] b on a.ExposureSetSID = b.ExposureSetSID ' \
                 'JOIN [SKExp].[dbo].[tContract] c on a.ContractSID = c.ContractSID ' \
                 'WHERE b.ExposureSetName = ' + "'" + exposure_name + "'"
        return copy.deepcopy(pd.read_sql(script, self.connection))
