#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Loss Mod Validation Script
******************************************************

"""

# Import standard Python packages and read outfile
import getopt
import sys

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print "Outfile: " + OUTFILE
OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\EPValidation.csv'
if OUTFILE is None:
    print ('Outfile is not passed')
    sys.exit()

# Import standard Python packages and read outfile
import time
import logging

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

HANDLER_INF0 = logging.FileHandler(OUTFILE[:-4] + '-info.log')
HANDLER_INF0.setLevel(logging.INFO)
LOGGER.addHandler(HANDLER_INF0)

# Import internal packages
from ValidationLib.analysis.main import *


__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10 |Anaconda 2.3.0 (64-bit)'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'

def file_skeleton(outfile):

    pd.DataFrame(columns=['CatalogTypeCode', 'ModelCode', 'PortGuSD', 'CalculatedPortGuSD',
                          'DifferencePortGuSD_Percent', 'PortGrSD', 'CalculatedPortGrSD',
                          'DifferencePortGrSD_Percent', 'Status']).to_csv(outfile, index=False)

# Extract the given arguments
try:
    server = 'QAWUDB2\SQL2012'
    result_db = 'SK_Res'
    analysis_name = 'LossMod_082110057291_LossMod_082110054338'
    tolerance = float('1')
except:
    LOGGER.error('Please verify the inputs')
    file_skeleton(OUTFILE)
    sys.exit()
print(sys.argv)
if __name__ == '__main__':

    start = time.time()

    LOGGER.info('**********************************************************************************')
    LOGGER.info('                        Loss Mod Validation Tool                                  ')
    LOGGER.info('**********************************************************************************')

    # Initialize the connection with the server
    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 1. Establishing the connection with database and initialize the Correlation class')
    try:
        db = Database(server)
        loss_mod = loss_mod(server)
    except:
        LOGGER.error('Invalid server information')
        file_skeleton(OUTFILE)
        sys.exit()
    LOGGER.info('Connection Established with server: ' + str(server))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 2. Get analysis sid')
    analysis_sid = db.analysis_sid(analysis_name)
    LOGGER.info('Analysis SID for analysis (MOD)' + str(analysis_name) + ' is ' + str(analysis_sid))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 3. Get  base analysis sid, loss mod template id and list of perils')
    base_analysis_sid = db.mod_analysis_sid(analysis_sid)
    template_id = db.loss_mod_temp_id(analysis_sid)
    peril_analysis = db.perils_analysis(analysis_sid)
    LOGGER.info('1. Base Analysis SID: ' + str(base_analysis_sid))
    LOGGER.info('2. Loss Mod Template SID: ' + str(template_id))
    LOGGER.info('3. Peril code: ' + str(peril_analysis))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 4. Get  Loss mod information')
    perilsTemp, coverage, LOB, admin_boundary, occupancy, construction, \
    yearBuilt, stories, contractID, locationID, factor = db.loss_mod_info(template_id)
    LOGGER.info('1. Perils: ' + str(perilsTemp))
    LOGGER.info('2. Coverage: ' + str(coverage))
    LOGGER.info('3. LOB: ' + str(LOB))
    LOGGER.info('4. Admin Boundaries: ' + str(admin_boundary))
    LOGGER.info('5. Occupancy Code: ' + str(occupancy))
    LOGGER.info('6. Construction Code: ' + str(construction))
    LOGGER.info('7. Year Built: ' + str(yearBuilt))
    LOGGER.info('8. Stories: ' + str(stories))
    LOGGER.info('9. ContractIDs: ' + str(contractID))
    LOGGER.info('10. LocationIDs: ' + str(locationID))
    LOGGER.info('11. Factor: ' + str(factor))


    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 5. Get result sid')
    baseResultSID = db.result_sid(base_analysis_sid)
    modResultSID = db.result_sid(analysis_sid)
    LOGGER.info('1. Base Result SID: ' + str(baseResultSID))
    LOGGER.info('2. Mod Result SID: ' + str(modResultSID))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 6. Group analysis perils')
    perilsAnalysisGrouped = db.group_analysis_perils(analysis_sid, perilsTemp)
    LOGGER.info('1. Grouped Perils used in analysis: ' + str(perilsAnalysisGrouped))

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 7. Check Rule')
    template_info = loss_mod.check_rule(analysis_sid, perilsAnalysisGrouped, coverage,
                                                  LOB, admin_boundary, occupancy, construction, yearBuilt,
                                                  stories, contractID, locationID, factor, modResultSID, result_db)

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 8. Get loss data frame')
    resultDF = loss_mod.get_loss_df(analysis_sid, result_db, baseResultSID, modResultSID, coverage)

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 9. Get loss data frame')
    validatedDF = loss_mod.validate(resultDF, template_info, coverage, analysis_sid, tolerance)

    LOGGER.info('**********************************************************************************')
    LOGGER.info('Step 10. Saving the results')
    validatedDF.to_csv(OUTFILE, index=False)


    LOGGER.info('----------------------------------------------------------------------------------')
    LOGGER.info('            Loss Mod Validation Completed Successfully                            ')
    LOGGER.info('----------------------------------------------------------------------------------')

    print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')