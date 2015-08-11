__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

import sys
from ValidationLib.general.main import *

if __name__ == '__main__':

    """
    """
    input_file = sys.argv[3] # 'C:\Users\i56228\Documents\Python\Git\ValidationLib\Scripts\Exposure Management\P2_LocationData.csv'
    file_type = sys.argv[4] # 'CSV' or 'UPX'
    outfile = sys.argv[5] # 'C:\Users\i56228\Documents\Python\Git\ValidationLib\Scripts\Exposure Management\P2_LocationData-Updated.csv'

    if file_type == 'CSV':
        csv2ui(input_file, outfile)