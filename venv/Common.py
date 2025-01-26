import datetime
import logging
import math
import pathlib

# region File Paths

flightInfoFilePath = '..\\Rohdaten\\flight_information.csv'
groundInfoFilePath = '..\\Rohdaten\\ground_information.csv'
exportedJoinDataFilePath = 'Export\\joinedData.csv'
transformedDataFilePath = 'Export\\transformedData.csv'
modelParameterFilePath = 'Export\\exportedModel.pkl'
# endregion File Paths

csvFileExtension = '.csv'

groundAirport = "East Carmen"

# region Thresholds
threshold_individual_values_per_column = 0.0001

# the threshold for missing values,
# e.g.: for 0.01, it means that a maximum of 1% of the values may be missing values to be filled
threshold_for_missing_values = 0.01
# endregion Thresholds

# region Column Names
columnName_m_onblockdt = "m_onblockdt"
columnName_m_offblockdt = "m_offblockdt"
columnName_dep_sched_time = "dep_sched_time"
columnName_arr_sched_time = "arr_sched_time"
columnName_dep_sched_date = "dep_sched_date"
columnName_arr_sched_date = "arr_sched_date"
columnName_dep_ap_sched = "dep_ap_sched"
columnName_arr_ap_sched = "arr_ap_sched"
columnName_day_of_origin = "day_of_origin"
columnName_TLC_trans = "TLC_trans"
columnName_crew_type_change = "crew_type_change"
columnName_arr_leg_inbound = "arr_leg_inbound"
columnName_sched_inbound_dep = "sched_inbound_dep"
columnName_sched_inbound_arr = "sched_inbound_arr"
columnName_sched_outbound_dep = "sched_outbound_dep"
columnName_sched_outbound_arr = "sched_outbound_arr"
columnName_leg_no = "leg_no"

flightInfo_columnNames_to_identify_duplicate_rows = [columnName_dep_ap_sched, columnName_arr_ap_sched,
                                                     columnName_arr_sched_date, columnName_dep_sched_date,
                                                     columnName_dep_sched_time, columnName_arr_sched_time]
groundInfo_columnNames_to_identify_duplicate_rows = ['sched_inbound_arr', 'sched_inbound_dep', 'sched_outbound_dep', 'fn_number']
columns_to_merge = {columnName_dep_sched_date: columnName_dep_sched_time,
                    columnName_arr_sched_date: columnName_arr_sched_time}

columns_to_remove = {columnName_TLC_trans,
                     columnName_crew_type_change}
colums_to_merge_value_separator = ' '

columnName_TargetData = columnName_m_onblockdt

columnNames_to_use_for_numeric_outliers = {'Sched Groundtime',
                                           'Act Groundtime',
                                           'catering_duration'}
# endregion Column Names

# region Column Renaming

columns_to_rename = {
  "Ac Type Code": "ac_type",
  "leg_inbound": "leg_no"
}
# endregion Column Renaming

# region Formats
datetimeformat = '%Y-%m-%d %H:%M:%S'
datetimeformatWithoutSeconds = '%Y-%m-%d %H:%M'
dateformat = '%Y-%m-%d'
timeformat = '%H:%M'
# endregion Formats

# region Logging
# initialize the file name for the log file
LogFileName = f'Logging\\LogFile_{datetime.datetime.today().strftime("%Y-%m-%d")}.log'
# initialize the logging basic configuration
logging.basicConfig(filename=LogFileName,
                    format='[%(asctime)s]: [%(levelname)s]\t - [%(pathname)s, LineNo: %(lineno)d] - %(message)s',
                    encoding='utf-8',
                    level=logging.DEBUG)
logging.info('Started new program run.')
logging.info('Initialized logging basic configuration.')

# endregion Logging
