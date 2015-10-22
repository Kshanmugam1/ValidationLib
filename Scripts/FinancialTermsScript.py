__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10 |Anaconda 2.3.0 (64-bit)'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

from ValidationLib.database.main import Database

server = 'QA-NGP-SQL\SQL2014'
resultdb = 'SKRes'
exposure_db = 'SKExp'
analysis_name = 'FinancialTerms5 - Geospatial Analysis_1'
exposure_name = 'FinancialTerms5'

db = Database(server)
analysis_sid = db.analysis_sid(analysis_name)
resultsid = db.result_sid(analysis_sid)
analysis_information = db.analysis_option(analysis_sid)

print(analysis_information)

location_information = db.get_loc_info_policy(exposure_db, exposure_name, resultdb, resultsid)

location_information['SumDeductible'] = location_information['Deductible1'] + location_information['Deductible2'] + \
                                        location_information['Deductible3'] + location_information['Deductible4']
location_information['SumLimit'] = location_information['Limit1'] + location_information['Limit2'] + \
                                   location_information['Limit3'] + location_information['Limit4']

location_information['ExpectedGross'] = 0.0
for i in range(len(location_information)):

    if location_information['LimitTypeCode'][i] == 'S':
        if location_information['DeductibleTypeCode'][i] == 'S':
            if location_information['Deductible1'][i] > 1:
                if location_information['AIROccupancyCode'][i] == 311:
                    location_information.loc[i, 'ExpectedGross'] = min(max((location_information['exposedGroundUp'][i] -
                                                                            location_information['SumDeductible'][i]),
                                                                           0),
                                                                       location_information['SumLimit'][i])
            else:
                if location_information['AIROccupancyCode'][i] == 311:
                    location_information.loc[i, 'ExpectedGross'] = min(max((location_information['exposedGroundUp'][i] -
                                                                            location_information['SumDeductible'][i]),
                                                                           0),
                                                                       location_information['SumLimit'][i])




        elif location_information['DeductibleTypeCode'][i] == 'C' and location_information['Deductible1'][i] < 1:
            coverage = ['A', 'B', 'C', 'D']
            ded = []
            for j in range(len(coverage)):
                if location_information['ReplacementValue' + coverage[j]][i] > 0.0:
                    ded.append(
                        location_information['Deductible' + str(j + 1)][i] * location_information['Limit' + str(j + 1)][
                            i])
                else:
                    ded.append(0)
            ded = sum(ded)
            location_information.loc[i, 'ExpectedGross'] = \
                min(max((location_information['exposedGroundUp'][i] - ded), 0), location_information['SumLimit'][i])
