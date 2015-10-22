__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10 |Anaconda 2.3.0 (64-bit)'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

from ValidationLib.database.main import Database

server = 'QAWUDB2\SQL2012'
resultdb = 'SKRes'
exposure_db = 'SKExp'
analysis_name = 'SS_Amount - Geospatial Analysis'
exposure_name = 'SS_Amount'

db = Database(server)
analysis_sid = db.analysis_sid(analysis_name)
resultsid = db.result_sid(analysis_sid)

location_information = db.get_loc_info_policy(exposure_db, exposure_name, resultdb, resultsid)

location_information['ExpectedGross'] = 0.0
for i in range(len(location_information)):

    if location_information['LimitTypeCode'][i] == 'S':

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
                limit.append(location_information['Limit' + str(j + 1)][i])  # * location_information['DamageRatio'][i]

            ded = sum(ded)
            limit = sum(limit)
            # Add the condition for Residential Occupancy
            if location_information['AIROccupancyCode'][i] == 311:
                location_information.loc[i, 'ExpectedGross'] = \
                    min(max((location_information['exposedGroundUp'][i] - ded), 0), limit)

        elif location_information['DeductibleTypeCode'][i] == 'C':
            coverage = ['A', 'B', 'C', 'D']
            total_gross = []
            for j in range(len(coverage)):
                ded = 0.0
                limit = 0.0
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
            location_information.loc[i, 'ExpectedGross'] = sum(total_gross)

location_information.to_csv('Sample.csv')
