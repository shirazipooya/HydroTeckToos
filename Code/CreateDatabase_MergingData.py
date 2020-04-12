

# Load Required Libraries

import pandas as pd
from glob import glob


# Load Data

# Select *.xlsx Files Location

OS = "UBUNTU"

if OS == "WINDOWS":
    DATA_PATH = "C:/w/HydroTech/Data/Processed_Data/"
else:
    DATA_PATH = "/home/pooya/w/HydroTech/Data/Processed_Data/"

# Read *.xlsx Files Name

xlsxFileNames = glob(DATA_PATH + '*.xlsx')

# Load All *.xlsx Files As A List

data = [pd.read_excel(xlsxFile) for xlsxFile in xlsxFileNames]

# Join All Data

data = pd.concat(data, ignore_index=True)
print(f'shape data is {data.shape}')

# Test Number Of Row

n_row = 0
for i in xlsxFileNames:
    df = pd.read_excel(i)
    n_row += df.shape[0]
    print(i)
    print(f'shape file is {df.shape}')

if n_row == data.shape[0]:
    print('DATA IS OK!')
else:
    print('YOU HAVE A PROBLEM!')


# Save Data

data.to_excel(DATA_PATH + 'Merged_Data.xlsx', header=True, index=False)
