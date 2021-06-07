from data.data_processing import data_processing
from data.data_reading import data_reading

init_dict = {"groups": "group_4", "binsize": 5000}

data_reading()
data_processing(init_dict)
