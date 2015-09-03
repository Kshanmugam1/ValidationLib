#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

******************************************************
Disaggregation Validation Script
******************************************************

"""

# Import standard Python packages and read outfile
import time
import logging

from ValidationLib.general.main import *
from ValidationLib.database.main import *

# Extract the given arguments
server = 'QAWUDB2\SQL2012'
result_db = 'SK_Res'
exposure_db = 'SK_Exp'
analysis_name = 'Can_AllLOB_1loc_overwrite - Loss Analysis_2'
exposure_name = 'Can_AllLOB_1loc'

geo_level_ranks = {'COUN':1, 'AREA':2, 'CRES':2, 'SUBA':3, 'POST':4, 'SUBA2':5}

db = Database(server)

loc_info = db.location_info(exposure_db, exposure_name)
analysis_sid = db.analysis_sid(analysis_name)
staging_location_tables = db.table_names('AIRWork',
                                            criteria = 't' + str(analysis_sid) + '%LOSS_StagingLocation%')
staging_contract_tables = db.table_names('AIRWork',
                                            criteria = 't' + str(analysis_sid) + '%LOSS_StagingContract%')
staging = pd.DataFrame()
for i in range(len(staging_location_tables['TABLE_NAME'].values)):
    staging = pd.concat([staging, db.staging_contract_location('AIRWork',
                                                staging_location_tables['TABLE_NAME'].values[i],
                                                staging_contract_tables['TABLE_NAME'].values[i])], axis=0).reset_index()

staging.drop('index', axis=1, inplace=True)
staging.set_index(['Latitude', 'Longitude'], inplace=True)
# child_staging = staging.loc[staging['LocationTypeCode']=='D', :].sort(['Latitude', 'Longitude']).reset_index()
# child_staging.drop('index', axis=1, inplace=True)

calculatedReplacement = pd.DataFrame()
child_staging = pd.DataFrame()
for i in range(len(loc_info)):

    calculatedReplacement = pd.concat([calculatedReplacement, db.disagg_loss(loc_info.iloc[i, :].values)], axis=0)

calculatedReplacement = calculatedReplacement.sort(['Latitude', 'Longitude']).reset_index()
calculatedReplacement.drop('index', axis=1, inplace=True)


calculatedReplacement_min_risk = calculatedReplacement.loc[(calculatedReplacement['ReplacementValueA'] >
                                                            calculatedReplacement['dblMinCovA']) &
                                                           (calculatedReplacement['ReplacementValueB'] >
                                                            calculatedReplacement['dblMinCovB']) &
                                                           (calculatedReplacement['ReplacementValueC'] >
                                                            calculatedReplacement['dblMinCovC']) &
                                                           (calculatedReplacement['ReplacementValueD'] >
                                                            calculatedReplacement['dblMinCovD']), :]

calculatedReplacement_min_risk = calculatedReplacement_min_risk.reset_index()
calculatedReplacement_min_risk.drop('index', axis=1, inplace=True)
calculatedReplacement_min_risk.set_index(['Latitude', 'Longitude'], inplace=True)

pd.concat([staging, calculatedReplacement_min_risk], axis=1, keys=['Latitude', 'Longitude']).to_csv('sample.csv', index=False)
