SELECT 
b.GeographySID, 
b.AIROccupancyCode, 
b.LocationID, 
g.ContractID, 
b.CountryCode, 
c.GeoLevelCode, 
e.ExchangeRate, 
f.intWeightTypeId, 
b.ReplacementValueA, 
b.ReplacementValueB, 
b.ReplacementValueC, 
b.ReplacementValueD 

FROM 
(SELECT [ExposureSetSID], [ExposureSetName], [StatusCode], [EnteredDate], [EditedDate], [Description], [RowVersion] 
	FROM [SK_Exp].[dbo].[tExposureSet] 
	WHERE ExposureSetName = 'Can_AllLOB_1LOC') a 

JOIN [SK_Exp].[dbo].[tLocation] b ON a.ExposureSetSID = b.ExposureSetSID 
JOIN [AIRGeography].[dbo].[tGeography] c on b.GeographySID = c.GeographySID 
JOIN [AIRReference].[dbo].[tCountryCurrencyXref] d on c.CountryCode=d.CountryCode 
JOIN [AIRUserSetting].[dbo].[tCurrencyExchangeRateSetConversion] e on d.CurrencyCode=e.CurrencyCode 
JOIN [AIRGeography].[dbo].[TblOccAirWeightType_xref] f ON b.AIROccupancyCode = f.intOccAir 
JOIN [SK_Exp].[dbo].[tContract] g ON b.ContractSID = g.ContractSID where  e.CurrencyExchangeRateSetSID=1 and d.IsDefault=1

SELECT AnalysisSID from AIRProject.dbo.tAnalysis WHERE AnalysisName = 'Can_AllLOB_1LOC - Loss Analysis'