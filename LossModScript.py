__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

from DbConn.main import *
from LossMod.main import *
from CsvTools.main import _saveDFCsv


if __name__ == '__main__':

    server = 'QA-TS-TPZ-DB1\SQL2014'
    #server = sys.argv[1]
    result_Db = 'SKRes'
    #result_Db = sys.argv[2]
    analysis_SID = 172
    #analysis_SID = sys.argv[3]
    result_path =  r'C:\Users\i56228\Documents\Python\Git\Validation-Tools\LossMod'
    #path = sys.argv[4]

    # Initialize the connection with the server
    validation = dbConnection(server)
    LossModValidation = LossModValidation(server)

    baseAnalysisSID = validation._getBaseAnalysisSID(analysis_SID)
    templateID = validation._getLossModTemID(analysis_SID)
    perilsAnalysis = validation._getPerilsAnalysis(analysis_SID)

    perilsTemp, coverage, LOB, occupancy, construction, \
    yearBuilt, stories, contractID, locationID, factor = LossModValidation._getLossModInfo(templateID)

    baseResultSID = validation._getResultSID(baseAnalysisSID)
    modResultSID = validation._getResultSID(analysis_SID)

    perilsAnalysisGrouped = LossModValidation._groupAnalysisPerils(analysis_SID, perilsTemp)

    template_info = LossModValidation._checkRule(analysis_SID, perilsAnalysisGrouped, coverage,
                                                 LOB, occupancy, construction, yearBuilt,
                                                 stories, contractID, locationID, factor, modResultSID, result_Db)

    resultDF = LossModValidation._getLossDF(analysis_SID, result_Db, baseResultSID, modResultSID, coverage)

    validatedDF = LossModValidation._validate(resultDF, template_info, coverage, analysis_SID)

    _saveDFCsv(validatedDF, result_path, 'LossModValidation')
