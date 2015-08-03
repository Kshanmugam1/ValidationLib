# Import standard Python packages
import getopt
import time
import sys

# Import internal packages
from ValidationLib.general.CsvTools.main import _saveDFCsv
from ValidationLib.database.main import *
from ValidationLib.analysis.LossMod.main import *

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'


if __name__ == '__main__':

    start = time.time()

    print('**********************************************************************************')
    print('                        Loss Mod Validation Tool                                  ')
    print('**********************************************************************************')

    # Extract the given parameters
    '''

    Input:

    1. Arg(3) - Server: 'QAWUDB2\SQL2012'
    2. Arg(4) - Result DB 'SSG_LossMod_Res'
    3. Arg(5) - Result Path
    4. Arg(6) - Analysis SID 730
    5. Arg(7) - Tolerance 1

    Output:

    1. Summary File

    '''

    server = sys.argv[3]
    result_Db = sys.argv[4]

    optlist, args = getopt.getopt(sys.argv[1:], [''], ['outfile='])
    outfile = None
    for o, a in optlist:
        if o == "--outfile":
            outfile = a
        print ("Outfile: " + outfile)
    if outfile is None:
        raise Exception("outfile not passed into script")

    analysis_SID = sys.argv[6]
    tolerance = sys.argv[7]

    # Initialize the connection with the server
    validation = dbConnection(server)
    LossModValidation = LossModValidation(server)

    print('**********************************************************************************************************')
    print('Step 1. Getting Base analysis ID, Loss Mod Template ID and List of Perils used in the analysis')
    baseAnalysisSID = validation._getBaseAnalysisSID(analysis_SID)
    templateID = validation._getLossModTemID(analysis_SID)
    perilsAnalysis = validation._getPerilsAnalysis(analysis_SID)
    print('1. Base Analysis SID: ' + str(baseAnalysisSID))
    print('1. Loss Mod Template SID: ' + str(templateID))
    print('1. Peril code: ' + str(perilsAnalysis))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 2. Getting the information from the loss mod template')
    perilsTemp, coverage, LOB, occupancy, construction, \
    yearBuilt, stories, contractID, locationID, factor = validation._getLossModInfo(templateID)
    print('1. Perils: ' + str(perilsTemp))
    print('2. Coverage: ' + str(coverage))
    print('3. LOB: ' + str(LOB))
    print('4. Occupancy Code: ' + str(occupancy))
    print('5. Construction Code: ' + str(construction))
    print('6. Year Built: ' + str(yearBuilt))
    print('7. Stories: ' + str(stories))
    print('8. ContractIDs: ' + str(contractID))
    print('9. LocationIDs: ' + str(locationID))
    print('10. Factor: ' + str(factor))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 3. Getting result SIDs from the analysis SID')
    baseResultSID = validation._getResultSID(baseAnalysisSID)
    modResultSID = validation._getResultSID(analysis_SID)
    print('1. Base Result SID: ' + str(baseResultSID))
    print('2. Mod Result SID: ' + str(modResultSID))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 4. Grouping analysis Perils')
    perilsAnalysisGrouped = validation._groupAnalysisPerils(analysis_SID, perilsTemp)
    print('1. Grouped Perils used in analysis: ' + str(perilsAnalysisGrouped))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 5. Checking rule')
    template_info = LossModValidation._check_rule(analysis_SID, perilsAnalysisGrouped, coverage,
                                                  LOB, occupancy, construction, yearBuilt,
                                                  stories, contractID, locationID, factor, modResultSID, result_Db)
    print('Rule Validated!!')
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 6. Getting the loss numbers')
    resultDF = LossModValidation._getLossDF(analysis_SID, result_Db, baseResultSID, modResultSID, coverage)
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 7. Validating the results')
    validatedDF = LossModValidation._validate(resultDF, template_info, coverage, analysis_SID, tolerance)
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 8. Saving the results')
    _saveDFCsv(validatedDF, outfile)
    print('**********************************************************************************************************')

    print('----------------------------------------------------------------------------------------------------------')
    print('                                             Validation Complete')
    print('----------------------------------------------------------------------------------------------------------')

    print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')