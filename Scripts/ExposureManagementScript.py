#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
CATXOL Validation Script
******************************************************

"""
# Import standard Python packages and read outfile
import getopt
import sys

# sys.path.insert(0, r'\\qafile2\TS\Working Data\Shashank\Validation Library\ValidationLib')
import datetime
import warnings

warnings.filterwarnings('ignore')

OPTLIST, ARGS = getopt.getopt(sys.argv[1:], [''], ['outfile='])

# OUTFILE = None
# for o, a in OPTLIST:
#     if o == "--outfile":
#         OUTFILE = a
#     print ("Outfile: " + OUTFILE)
#
# if OUTFILE is None:
#     print ('Outfile is not passed')
#     sys.exit()

# Import standard Python packages and read outfile
import time
import logging

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

# HANDLER_INFO = logging.FileHandler(OUTFILE[:-4] + '-info.log')
# HANDLER_INFO.setLevel(logging.INFO)
# LOGGER.addHandler(HANDLER_INFO)

# Import internal packages
from ValidationLib.general.main import *
from ValidationLib.database.main import *

__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Anaconda - Python 2.7.10 64 bit'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Complete'



# Extract the given arguments
try:

    server = 'qa-ts-temp-db1\sql2012'
    exposure_db = 'SKExp'
    exposure_set_name = 'exposure_management_111212436407'

    location_file = r'\\qafile2\TS\Working Data\Shashank\4.0\Inputs\ExposureManagement\Location_copy.csv'
    aie_log = r'\\qafile2\TS\Working Data\Shashank\ExposureManagement\exposure_management_Csv_Import_Log_1029526.txt'
    exp_log = r'\\qafile2\TS\Working Data\Shashank\ExposureManagement\exposure_management_ToolTipInformation_1029526.txt'

except:
    LOGGER.error('Please verify the inputs')
    sys.exit()

if __name__ == "__main__":

    start = time.time()

    LOGGER.info('********************************')
    LOGGER.info('**      Touchstone v.3.0      **')
    LOGGER.info('********************************')

    LOGGER.info('\n********** Log header **********\n')
    LOGGER.info('Description:   CATXOL Validation')
    LOGGER.info('Time Submitted: ' + str(datetime.datetime.now()))
    LOGGER.info('Status:                Completed')

    LOGGER.info('\n********** Log Import Options **********\n')
    # Initialize the connection with the server
    try:
        db = Database(server)
        LOGGER.info('Server: ' + str(server))
    except:
        LOGGER.error('Error: Check connection to database server')
        sys.exit()

    exposure_file = pd.read_csv(location_file)
    import_data = db.exp_location_information(exposure_db, exposure_set_name)

    parse_log_files(aie_log, aie_log[:-4] + '_error_log.txt')
    aie_error_data = pd.read_csv(aie_log[:-4] + '_error_log.txt', header=None, sep=',')
    aie_error_data.columns = ['ContractID', 'LocationID', 'Error']
    aie_exposure_data = import_data.loc[~import_data.ContractID.str.contains('Copy_'), :]

    with open(exp_log[:-4] + "_Updated.txt", 'wb') as out:
        with open(exp_log, "r") as file:
            data = file.readlines()
            for i in range(len(data)):
                if not '-' in data[i]:
                    x = data[i]
                    for j in range(len(data) - i):
                        if '-' not in data[i + j + 1]:
                            x = "".join((x + data[i + j + 1]).split('\n'))

                        else:
                            break
                    out.writelines(x)

    exp_error_data = pd.read_csv(exp_log[:-4] + '_Updated.txt', header=None, sep=',')
    exp_error_data = exp_error_data.drop(exp_error_data.columns[[2]], axis=1)
    exp_error_data.columns = ['ContractID', 'LocationID', 'Error']
    exp_exposure_data = import_data.loc[import_data.ContractID.str.contains('Copy_'), :]

    print(pd.merge(aie_exposure_data, exp_exposure_data, on=['LocationID'], how='outer').to_csv('sample.csv'))
