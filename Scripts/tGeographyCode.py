__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10 |Anaconda 2.3.0 (64-bit)'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

from ValidationLib.database.main import *
import itertools, copy

def find_indices(lst, condition):
    return [i for i, elem in enumerate(lst) if condition(elem)]

def swap_data(data, target, type=None):

    master_data = pd.DataFrame()
    original_data = copy.deepcopy(locations)
    for j in range(len(table)):
                swap_position = find_indices(table.iloc[j, :].values, lambda e: e==True)
                for k in range(len(swap_position)):
                    data[swap_position[k]] = 'X'

                master_data = master_data.append([data])
                # print(locations)
                data = copy.deepcopy(original_data)

    return master_data


server = 'QA-NGP-DB1'
db = Database(server)
countries = db.get_country_list()
master_data = pd.DataFrame()

for i in range(len(countries)):

    country_code = countries.iloc[i, 0]
    country_resolution = db.get_country_resolution(country_code).iloc[:, 0].values
    print(country_resolution)

    if 'SUB2' in country_resolution:
        distinct_code_lengths = db.get_areacode_length('SUB2', country_code).dropna().iloc[:, 0]
        #Level 4 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'SUB2', length).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=5)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])

    if 'POST' in country_resolution:
        distinct_code_lengths = db.get_areacode_length('POST', country_code).dropna().iloc[:, 0]
        #Level 4 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'POST', length).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=5)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])

    if 'SUBA' in country_resolution:
        distinct_code_lengths = db.get_areacode_length('SUBA', country_code).dropna().iloc[:, 0]
        #Level 4 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'SUBA', length).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=5)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])
        #Level 3 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'SUBA', length, level=3).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=3)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])

    if 'CRES' in country_resolution:
        distinct_code_lengths = db.get_areacode_length('CRES', country_code).dropna().iloc[:, 0]
        #Level 4 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'CRES', length).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=5)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])

        #Level 3 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'CRES', length, level=3).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=3)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])

        #Level 2 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'CRES', length, level=2).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=2)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])
        print(locations)
    if 'AREA' in country_resolution:
        distinct_code_lengths = db.get_areacode_length('AREA', country_code).dropna().iloc[:, 0]
        #Level 4 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'AREA', length).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=5)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])

        #Level 3 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'AREA', length, level=3).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=3)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])

        #Level 2 Data
        for length in distinct_code_lengths:
            locations = db.get_areacode_length_data(country_code, 'AREA', length, level=2).iloc[0, :]
            target = []
            table = pd.DataFrame(list(itertools.product([False, True], repeat=2)))
            master_data = pd.concat([master_data, swap_data(copy.deepcopy(locations), target)])

print master_data.to_csv('sample.csv')

