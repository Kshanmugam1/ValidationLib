# Import standard Python packages
import time

# Import internal packages
from ValidationLib.analysis.main import *

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

    server = 'QAWUDB2\SQL2012'
    result_Db = 'SKResult'

    # optlist, args = getopt.getopt(sys.argv[1:], [''], ['outfile='])
    # outfile = None
    # for o, a in optlist:
    #     if o == "--outfile":
    #         outfile = a
    #     print ("Outfile: " + outfile)
    # if outfile is None:
    #     raise Exception("outfile not passed into script")
    outfile = 'C:\Users\i56228\Documents\Python\Git\ValidationLib\LossModValidation.csv'
    analysis_SID = 993
    tolerance = 1

    # Initialize the connection with the server
    validation = Database(server)
    LossModValidation = LossMod(server)

    print('**********************************************************************************************************')
    print('Step 1. Getting Base analysis ID, Loss Mod Template ID and List of Perils used in the analysis')
    baseAnalysisSID = validation.mod_analysis_sid(analysis_SID)
    templateID = validation.loss_mod_temp_id(analysis_SID)
    perilsAnalysis = validation.perils_analysis(analysis_SID)
    print('1. Base Analysis SID: ' + str(baseAnalysisSID))
    print('1. Loss Mod Template SID: ' + str(templateID))
    print('1. Peril code: ' + str(perilsAnalysis))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 2. Getting the information from the loss mod template')
    perilsTemp, coverage, LOB, admin_boundary, occupancy, construction, \
    yearBuilt, stories, contractID, locationID, factor = validation.loss_mod_info(templateID)
    print('1. Perils: ' + str(perilsTemp))
    print('2. Coverage: ' + str(coverage))
    print('3. LOB: ' + str(LOB))
    print('4. Admin Boundaries: ' + str(admin_boundary))
    print('5. Occupancy Code: ' + str(occupancy))
    print('6. Construction Code: ' + str(construction))
    print('7. Year Built: ' + str(yearBuilt))
    print('8. Stories: ' + str(stories))
    print('9. ContractIDs: ' + str(contractID))
    print('10. LocationIDs: ' + str(locationID))
    print('11. Factor: ' + str(factor))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 3. Getting result SIDs from the analysis SID')
    baseResultSID = validation.result_sid(baseAnalysisSID)
    modResultSID = validation.result_sid(analysis_SID)
    print('1. Base Result SID: ' + str(baseResultSID))
    print('2. Mod Result SID: ' + str(modResultSID))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 4. Grouping analysis Perils')
    perilsAnalysisGrouped = validation.group_analysis_perils(analysis_SID, perilsTemp)
    print('1. Grouped Perils used in analysis: ' + str(perilsAnalysisGrouped))
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 5. Checking rule')
    template_info = LossMod.check_rule(analysis_SID, perilsAnalysisGrouped, coverage,
                                                  LOB, admin_boundary, occupancy, construction, yearBuilt,
                                                  stories, contractID, locationID, factor, modResultSID, result_Db)
    print('Rule Validated!!')
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 6. Getting the loss numbers')
    resultDF = LossMod.get_loss_df(analysis_SID, result_Db, baseResultSID, modResultSID, coverage)
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 7. Validating the results')
    validatedDF = LossMod.validate(resultDF, template_info, coverage, analysis_SID, tolerance)
    print('**********************************************************************************************************')

    print('**********************************************************************************************************')
    print('Step 8. Saving the results')
    validatedDF.to_csv(outfile, index=False)
    print('**********************************************************************************************************')

    print('----------------------------------------------------------------------------------------------------------')
    print('                                             Validation Complete')
    print('----------------------------------------------------------------------------------------------------------')

    print('********** Process Complete: ' + str(time.time() - start) + ' Seconds **********')