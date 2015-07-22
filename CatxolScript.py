__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

# Import internal packages
from DbConn.main import *
from Catxol.main import *

if __name__ == '__main__':

    print('**********************************************************************************')
    print('                          CATXOL Validation Tool                                  ')
    print('**********************************************************************************')
    # Extract the given arguments
    '''
    1. Arg(3) - Server
    2. Arg(4) - Result DB
    3. Arg(5) - Result Path (As an Outfile)
    4. Arg(6) - Analysis SID
    '''

    server = 'QAWUDB2\SQL2012'
    result_Db = 'SKCatRes'
    result_path =  r'C:\Users\i56228\Documents\Python\Git\ValidationLib\Catxol_Validation.csv'
    analysis_SID = 620

    # Initialize the connection with the server
    validation = dbConnection(server)
    Program = ProgramValidation(server)

    print('**********************************************************************************************************')
    print('Step 1. Getting result SID')
    resultSID = validation._getResultSID(analysis_SID)

    print('**********************************************************************************************************')
    print('Step 2. Getting Program ID')
    programSID = validation._getProgramID(analysis_SID)

    print('**********************************************************************************************************')
    print('Step 3. Program Info')
    occ_limit, occ_ret, agg_limit, agg_ret = validation._getProgramInfo(programSID)
    print(occ_limit, occ_ret, agg_limit, agg_ret)
    print('**********************************************************************************************************')
    print('Step 5. Validate the Program')
    resultDF = Program._validate(result_Db, resultSID, occ_limit, occ_ret, agg_limit, agg_ret)
