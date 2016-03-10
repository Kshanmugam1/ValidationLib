__author__ = 'Shashank Kapadia'
__copyright__ = '2015 AIR Worldwide, Inc.. All rights reserved'
__version__ = '1.0'
__interpreter__ = 'Python 2.7.10 |Anaconda 2.3.0 (64-bit)'
__maintainer__ = 'Shashank kapadia'
__email__ = 'skapadia@air-worldwide.com'
__status__ = 'Production'

from ValidationLib.database.main import *
import itertools

def find_indices(lst, condition):
    return [i for i, elem in enumerate(lst) if condition(elem)]

def swap_data(data, target, type=None):
    master_data = pd.DataFrame()
    table = pd.DataFrame(list(itertools.product([False, True], repeat=8)))
    for j in range(len(table)):
                swap_position = find_indices(table.iloc[j, :].values, lambda e: e==True)
                for k in range(len(swap_position)):
                    data[swap_position[k]] = target[swap_position[k]]
                master_data = master_data.append([data])
                data = locations.iloc[0, :].values
    print(master_data)
    return master_data


server = 'QA-NGP-SQL\sql2014'
db = Database(server)
countries = db.get_country_list()
master_data = pd.DataFrame()
table = pd.DataFrame(list(itertools.product([False, True], repeat=8)))

for i in range(len(countries)):

    country_code = countries.iloc[i, 0]

    country_resolution = db.get_country_resolution(country_code).iloc[:, 0].values
    if 'SUB2' in country_resolution:
        locations = db.get_address_information(country_code, 'SUB2')
        if len(locations) >= 2:
            data = locations.iloc[0, :].values
            target = locations.iloc[1, :].value
            master_data = pd.concat([master_data, swap_data(data, target)])

        continue

    if 'POST' in country_resolution:
        locations =  db.get_address_information(country_code, 'POST')
        if len(locations) >= 2:
            data = locations.iloc[0, :].values
            target = locations.iloc[1, :].values
            master_data = pd.concat([master_data, swap_data(data, target)])
            data = locations.iloc[0, :].values

        continue

    if 'SUBA' in country_resolution:
        locations =  db.get_address_information(country_code, 'SUBA')
        if len(locations) >= 2:
            data = locations.iloc[0, :].values
            target = locations.iloc[1, :].values
            master_data = pd.concat([master_data, swap_data(data, target)])
            data = locations.iloc[0, :].values
        continue

    if 'CRES' in country_resolution:
        locations =  db.get_address_information(country_code, 'CRES')
        if len(locations) >= 2:
            data = locations.iloc[0, :].values
            target = locations.iloc[1, :].values
            master_data = pd.concat([master_data, swap_data(data, target)])
            data = locations.iloc[0, :].values
        continue

    if 'AREA' in country_resolution:
        locations =  db.get_address_information(country_code, 'AREA')
        if len(locations) >= 2:
            data = locations.iloc[0, :].values
            target = locations.iloc[1, :].values
            master_data = pd.concat([master_data, swap_data(data, target)])
            data = locations.iloc[0, :].values
        continue

master_data.to_csv('countries_locationBlankCode.csv', index=False)


