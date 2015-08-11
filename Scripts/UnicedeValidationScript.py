__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.9'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

from ValidationLib.general.main import *
import threading
from os import listdir
import time

if __name__ == '__main__':

    print('***************************************** Process Started *****************************************')
    path_to_dir = 'G:\UnicedeTest'

    files = listdir(path_to_dir)
    results_test = [str(path_to_dir + '\\' + filename) for filename in files if filename.endswith('.txt')]

    thread_list = []

    start_time = time.time()

    for i in range(len(results_test)):
        t = threading.Thread(target=UnicedeCompare, args=(results_test[i][:-4] + '_CATUnicedeReport_.CSV', results_test[i], path_to_dir,))
        thread_list.append(t)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    print('***************************************** Process Completed *****************************************')
    print('Time to process ' + str(len(results_test)) + ' files is ' + str(time.time() - start_time) + ' seconds.')