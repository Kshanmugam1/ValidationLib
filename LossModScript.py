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
import datetime

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

OUTFILE = None
for o, a in OPTLIST:
    if o == "--outfile":
        OUTFILE = a
    print "Outfile: " + OUTFILE
# OUTFILE = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\EPValidation.csv'
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
    server = sys.argv[3]
    result_db = sys.argv[4]
    analysis_name = sys.argv[5]
    tolerance = float(sys.argv[6])
except:
    LOGGER.error('Please verify the inputs')
    file_skeleton(OUTFILE)
    sys.exit()
print(sys.argv)
if __name__ == '__main__':

    start = time.time()

    LOGGER.info('********************************')
    LOGGER.info('**      Touchstone v.3.0      **')
    LOGGER.info('********************************')

    LOGGER.info('\n********** Log header **********\n')
    LOGGER.info('Description:   Loss Mod Validation')
    LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
    LOGGER.info('Status:                Completed')

    LOGGER.info('\n********** Log Import Options **********\n')
    # Initialize the connection with the server
    try:
        db = Database(server)
        loss_mod = loss_mod(server)
    except:
        LOGGER.error('Invalid server information')
        file_skeleton(OUTFILE)
        sys.exit()
    LOGGER.info('Server: ' + str(server))

    analysis_sid = db.analysis_sid(analysis_name) + 1
    LOGGER.info('Analysis SID: ' + str(analysis_sid))

    base_analysis_sid = db.mod_analysis_sid(analysis_sid)
    template_id = db.loss_mod_temp_id(analysis_sid)
    peril_analysis = db.perils_analysis(analysis_sid)
    LOGGER.info('Base Analysis SID: ' + str(base_analysis_sid))
    LOGGER.info('Loss Mod Template SID: ' + str(template_id))
    LOGGER.info('Peril code: ' + str(peril_analysis))

    baseResultSID = db.result_sid(base_analysis_sid)
    modResultSID = db.result_sid(analysis_sid)
    LOGGER.info('Base Result SID: ' + str(baseResultSID))
    LOGGER.info('Mod Result SID: ' + str(modResultSID))


    LOGGER.info('\n********** Loss Mod Options **********\n')
    perilsTemp, coverage, LOB, admin_boundary, occupancy, construction, \
    yearBuilt, stories, contractID, locationID, factor = db.loss_mod_info(template_id)
    LOGGER.info('Perils: ' + str(perilsTemp))
    LOGGER.info('Coverage: ' + str(coverage))
    LOGGER.info('LOB: ' + str(LOB))
    LOGGER.info('Admin Boundaries: ' + str(admin_boundary))
    LOGGER.info('Occupancy Code: ' + str(occupancy))
    LOGGER.info('Construction Code: ' + str(construction))
    LOGGER.info('Year Built: ' + str(yearBuilt))
    LOGGER.info('Stories: ' + str(stories))
    LOGGER.info('ContractIDs: ' + str(contractID))
    LOGGER.info('LocationIDs: ' + str(locationID))
    LOGGER.info('Factor: ' + str(factor))

    perilsAnalysisGrouped = db.group_analysis_perils(analysis_sid, perilsTemp)
    LOGGER.info('Grouped Perils used in analysis: ' + str(perilsAnalysisGrouped))

    template_info = loss_mod.check_rule(analysis_sid, perilsAnalysisGrouped, coverage,
                                                  LOB, admin_boundary, occupancy, construction, yearBuilt,
                                                  stories, contractID, locationID, factor, modResultSID, result_db)

    resultDF = loss_mod.get_loss_df(analysis_sid, result_db, baseResultSID, modResultSID, coverage)

    validatedDF = loss_mod.validate(resultDF, template_info, coverage, analysis_sid, tolerance)

    validatedDF.to_csv(OUTFILE, index=False)


    LOGGER.info('----------------------------------------------------------------------------------')
    LOGGER.info('            Loss Mod Validation Completed Successfully                            ')
    LOGGER.info('----------------------------------------------------------------------------------')

    print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')