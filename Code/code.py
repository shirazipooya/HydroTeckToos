
# %% Initial Settings: Load Required Libraries
import os
import sys
import platform
import pandas as pd
import numpy as np
from plydata import *

import geopandas as gpd
import json


# %% Initial Settings: Set Project Path
OS = platform.system()

if OS == "Linux":
    PROJECT_PATH = "/home/pooya/w/HydroTech/"
else:
    PROJECT_PATH = "c:/w/HydroTech/"


# %% Initial Settings: Load Required Functions

# Function 01 - Extract 'Region', 'District' from 'Peyman' Column
def extractRD(x, para):

    if para == 'ناحیه':
        if para not in x:
            return np.nan
        elif (x.index(para) + 1) >= len(x):
            return np.nan
        else:
            return str(x[x.index(para) + 1]).zfill(2)

    if para == 'منطقه':
        if para not in x:
            if ('کمربند' in x) and ('جنوبی' in x):
                return '14'
            elif ('کمربند' in x) and ('شمالی' in x):
                return '15'
            elif ('سازمان' in x) and ('پارک‌ها' in x):
                return '16'
            else:
                return np.nan
        elif (x.index(para) + 1) >= len(x):
            return np.nan
        else:
            if str(x[x.index(para) + 1]) == 'ثامن':
                return '13'
            else:
                return str(x[x.index(para) + 1]).zfill(2)


# %% Load Data: Read Data
raw_data = pd.read_excel(PROJECT_PATH + 'Data/Processed_Data/Merged_Data.xlsx')


# %% Data Cleansing: Remove Duplicated Rows
# 01. Report Duplicated Rows
tmp = raw_data[
    raw_data.duplicated(
        subset=list(raw_data.columns)[1:],
        keep=False
    )
]

tmp = tmp.sort_values(['پیمان', 'نام لکه', 'نوع قلم', 'زیرمجموعه هر قلم'])

tmp.to_excel(PROJECT_PATH + "Report/Duplicate_Rows.xlsx", index=False)

print(f"Total Number of Duplicate Rows in a Data: {tmp.shape[0]}")

tmp = raw_data[
    raw_data.duplicated(
        subset=list(raw_data.columns)[1:],
        keep='first')
]

print(f"Total Number of Duplicate Items in a Data: {tmp.shape[0]}")

del tmp

# 02. Remove Duplicated Rows
raw_data = raw_data.drop_duplicates(
    subset=list(raw_data.columns)[1:],
    keep='first'
)

print(f"Data Size: {raw_data.shape}")


# %% Data Cleansing: Remove Some Rows
tmp = raw_data["نوع آیتم"] == "حجمی"
tmp = tmp[tmp.values == True]

raw_data.drop(
    index=tmp.index,
    inplace=True
)

del tmp

print(f"Data Size: {raw_data.shape}")


# %% Data Cleansing: Extract Region And District
tmp = raw_data["پیمان"].str.strip().str.split()

raw_data["Region"] = tmp.apply(extractRD, para="منطقه")

raw_data["District"] = tmp.apply(extractRD, para="ناحیه")

del tmp


# %% Data Cleansing: Extract Peyman
# Extract Uniqe Peyman
tmp = raw_data.groupby(['Region', 'District'])['پیمان']
tmp = tmp.value_counts(dropna=False, sort=False)
tmp = pd.DataFrame(tmp)
tmp = tmp.rename(columns={'پیمان': 'Count'}).reset_index()

Peyman = []
for R in list(tmp['Region'].unique()):
    tmpR = tmp[tmp['Region'] == R]
    for D in list(tmpR['District'].unique()):
        tmpD = tmpR[tmpR['District'] == D]
        Peyman += list(range(1, len(tmpD) + 1))

tmp['Peyman'] = Peyman
tmp['Peyman'] = tmp['Peyman'].astype(str).str.zfill(2)

# Add Peyman To raw_data
tmp = tmp >> select('Region', 'District', 'پیمان', 'Peyman')

raw_data = pd.merge(raw_data,
                    tmp,
                    how='left',
                    on=['Region', 'District', 'پیمان'])

del tmp, tmpR, tmpD


# %% Data Cleansing: Extract Address
# Extract Uniqe Address
tmp = raw_data.groupby(['Region', 'District', 'Peyman'])['نام لکه']
tmp = tmp.value_counts(dropna=False, sort=False)
tmp = pd.DataFrame(tmp)
tmp = tmp.rename(columns={'نام لکه': 'Count'}).reset_index()

Address = []
for R in list(tmp['Region'].unique()):
    tmpR = tmp[tmp['Region'] == R]
    for D in list(tmpR['District'].unique()):
        tmpD = tmpR[tmpR['District'] == D]
        for P in list(tmpD['Peyman'].unique()):
            tmpP = tmpD[tmpD['Peyman'] == P]
            Address += list(range(1, len(tmpP) + 1))

tmp['Address'] = Address
tmp['Address'] = tmp['Address'].astype(str).str.zfill(3)

# Add Address To raw_data
tmp = tmp >> select('Region', 'District', 'Peyman', 'نام لکه', 'Address')

raw_data = pd.merge(raw_data,
                    tmp,
                    how='left',
                    on=['Region', 'District', 'Peyman', 'نام لکه'])

del tmp, tmpR, tmpD, tmpP


# %% Report: Check Region
raw_data['Region'].value_counts(dropna=False, sort=True)


# %% Report: Check District
raw_data['District'].value_counts(dropna=False, sort=True)


# %% Report: Check Peyman
raw_data['Peyman'].value_counts(dropna=False)


# %% Report: Check Address
raw_data['Address'].value_counts(dropna=False)


# %% Data Cleansing: Define Spots Class
Spots_Class = {
    'میادین': '01',
    'لچکی ها': '02',
    'آیلند های بزرگراه': '03',
    'آیلند ها': '04',
    'حاشیه های بزرگراه': '05',
    'حاشیه معابر': '06',
    'بوستان خطی': '07',
    'پارک های زیر 6 هکتار': '08',
    'پارک های بین 6 تا 10 هکتار': '09',
    'پارک های بالای 10 هکتار': '10',
    'جنگل کاری داخل محدوده': '11',
    'کمربندی': '12',
    'کمربند سبز حفاظتی': '13',
}


# %% Data Cleansing: Spots Class
# Extract All Spots Class From Data
tmp = raw_data['نوع لکه'].value_counts(dropna=False, sort=True)
tmp = tmp.reset_index().rename(columns={'index': 'نوع لکه',
                                        'نوع لکه': 'تعداد'})

tmp['Spot'] = list(map(Spots_Class.get, tmp['نوع لکه']))

file_name = PROJECT_PATH + 'Report/Type_of_Spots.xlsx'
tmp.to_excel(file_name, index=False)

# Add Spots Class to raw_data
tmp = tmp[['نوع لکه', 'Spot']]
raw_data = pd.merge(raw_data,
                    tmp,
                    how='left',
                    on=['نوع لکه'])

del tmp, file_name


# %% Data Cleansing: Define Irrigation Method Class
Irrigation_Method_Class = {
    'آبیاری ثقلی': '01',
    'آبیاری تانکری': '02',
    'آبیاری شلنگی': '03',
    'آبیاری تحت فشار': '04'
}


# %% Data Cleansing: Irrigation Method Class
# Extract All Irrigation Method Class From Data
tmp = raw_data['زیرمجموعه هر قلم'].value_counts(dropna=False, sort=True)
tmp = tmp.reset_index().rename(columns={'index': 'زیرمجموعه هر قلم',
                                        'زیرمجموعه هر قلم': 'تعداد'})

tmp['Irrigation'] = list(map(Irrigation_Method_Class.get,
                             tmp['زیرمجموعه هر قلم']))

# Add Irrigation Method Class to raw_data
tmp = tmp[['زیرمجموعه هر قلم', 'Irrigation']]
raw_data = pd.merge(raw_data,
                    tmp,
                    how='left',
                    on=['زیرمجموعه هر قلم'])

raw_data.fillna(value=np.nan, inplace=True)

del tmp


# %% Data Cleansing: Define Species Plant Class
Species_Plant_Class = {
    'چمن': '01',
    'گل دائم باغچه های معمولی': '02',
    'گل دائم فلاورباکسهای سطوح شیب دار': '04',
    'گل فصل باغچه های معمولی': '05',
    'پرچین': '06',
    'درخت و درختچه': '10',
    'درختان جنگلی': '12',
    'گل فصل فلاورباکس های سطوح شیب دار': '14'
}

# %% Data Cleansing: Species Plant Class
# Extract All Species Plant Class From Data
tmp = raw_data['نوع قلم'].value_counts(dropna=False, sort=True)
tmp = tmp.reset_index().rename(columns={'index': 'نوع قلم',
                                        'نوع قلم': 'تعداد'})

tmp['Species'] = list(map(Species_Plant_Class.get, tmp['نوع قلم']))

# Add Species Plant Class to raw_data
tmp = tmp[['نوع قلم', 'Species']]
raw_data = pd.merge(raw_data,
                    tmp,
                    how='left',
                    on=['نوع قلم'])
raw_data.fillna(value=np.nan, inplace=True)

del tmp


# %% Data Cleansing: Generate ID
# Check Number of NaN
raw_data[['Region', 'District', 'Peyman',
          'Address', 'Spot', 'Irrigation', 'Species']].isnull().sum()

# Generate ID
tmp = 'Region + District + Peyman + Address + "-" + Spot + Irrigation + Species'
raw_data = raw_data >> define(ID=tmp)

del tmp

# %% Report: ID Check
tmp = raw_data.astype(str).groupby(['ID']).size()
tmp = pd.DataFrame(tmp).rename(columns={0: 'Count'})
tmp >> query("Count >= 2")

del tmp

# %% Report: Save And Remove Duplicate ID
tmp = raw_data.dropna(subset=['ID']).duplicated(subset="ID", keep=False)
tmp = raw_data.dropna(subset=['ID'])[tmp.values]

file_name = PROJECT_PATH + '/Report/Duplicate_ID.xlsx'
tmp.to_excel(file_name, index=False)

tmp = raw_data.dropna(subset=['ID']).duplicated(subset="ID", keep='last')
tmp = tmp[tmp.values == True].index
raw_data.drop(axis=0, index=tmp, inplace=True)

del tmp, file_name

print(f"Data Size: {raw_data.shape}")


# %% Data Cleansing: Change Columns Name
data = raw_data >> select('ID', 'Region', 'District', 'Peyman', 'Address',
                          'Spot', 'Irrigation', 'Species', 'نمایش آخرین ریزمتره (ریزمتره نهایی)',
                          'مساحت لکه (مترمربع)', 'مساحت پیمان (مترمربع)', 'نوع آیتم', 'نوع لکه',
                          'نام لکه', 'نوع قلم', 'زیرمجموعه هر قلم', 'پیمان', 'ردیف')

data.rename(columns={
    'نمایش آخرین ریزمتره (ریزمتره نهایی)': 'Irrigation_Area',
    'مساحت لکه (مترمربع)': 'Address_Area',
    'مساحت پیمان (مترمربع)': 'Peyman_Area',
    'نوع آیتم': 'Item_Type',
    'نوع لکه': 'Lake_Type',
    'نام لکه': 'Lake_Name',
    'نوع قلم': 'Ghalam_Type',
    'زیرمجموعه هر قلم': 'Ghalam_SubType',
    'پیمان': 'Check_point',
    'ردیف': 'Radif'
}, inplace=True)


# %% Data Cleansing: Remove 'meter' From Some Rows
tmp = data >> query(
    'Ghalam_Type == "درخت و درختچه" and Ghalam_SubType == "متر"'
)

file_name = PROJECT_PATH + "Report/Error_Metr.xlsx"
tmp.to_excel(file_name, index=False)

data.loc[list(tmp.index), "Ghalam_SubType"] = "متر مربع"

del tmp, file_name


# %% Add Some Element To Database
# Define Extera Class
Extera_Class = {
    'متر مربع': '01',
    'اصله': '02'
}

# Extract All Extera Class In Data
tmp = data['Ghalam_SubType'].value_counts(dropna=False, sort=True)
tmp = tmp.reset_index().rename(columns={'index': 'Ghalam_SubType',
                                        'Ghalam_SubType': 'Count'})
tmp['Extera'] = list(map(Extera_Class.get, tmp['Ghalam_SubType']))

# Add Extera Class to data
tmp = tmp[['Ghalam_SubType', 'Extera']]

data = pd.merge(data,
                tmp,
                how='left',
                on=['Ghalam_SubType'])

data.fillna(value=np.nan, inplace=True)

# Remove From Irrigation_Area
data = data >> define(Tree=if_else('Extera == "02"',
                                   'Irrigation_Area',
                                   np.nan))

data = data >> define(M2=if_else('Extera == "01"',
                                 'Irrigation_Area',
                                 np.nan))

data = data >> define(Irrigation_Area=if_else('Extera == "02"',
                                              np.nan,
                                              'Irrigation_Area'))

data = data >> define(Irrigation_Area=if_else('Extera == "01"',
                                              np.nan,
                                              'Irrigation_Area'))

# %% Modified ID
data = data >> define(Species=if_else('Extera == "02"', '"13"', 'Species'))

s = 'Region + District + Peyman + Address + "-" + Spot + "00" + Species'
data = data >> define(ID=if_else('Extera == "02"', s, 'ID'))

data['Species'].replace(to_replace='nan', value=np.NaN, inplace=True)
data['ID'].replace(to_replace='nan', value=np.NaN, inplace=True)

data = data >> call('.dropna', subset=['ID'])


# %% Report: Check Region, District and Peyman
tmpG = data.astype(str).groupby(
    ['Region', 'District', 'Peyman', 'Check_point'])
tmp = pd.DataFrame(tmpG.size())
tmp = tmp.reset_index()
tmp = tmp.rename(columns={'Region': 'منطقه',
                          'District': 'ناحیه',
                          'Peyman': 'پیمان',
                          'Check_point': 'نام پیمان',
                          0: 'تعداد ردیف'})

file_name = PROJECT_PATH + "Report/Region_District_Peyman.xlsx"
tmp.to_excel(file_name, index=False)

print(tmp)
del tmpG, tmp, file_name

# %% Report: Check Region, District, Peyman, Address
tmpG = data.astype(str).groupby(
    ['Region', 'District', 'Peyman', 'Address', 'Lake_Name']
)
tmp = pd.DataFrame(tmpG.size())
tmp = tmp.reset_index()
tmp = tmp.rename(columns={'Region': 'منطقه',
                          'District': 'ناحیه',
                          'Peyman': 'پیمان',
                          'Address': 'لکه',
                          'Lake_Name': 'نام لکه',
                          0: 'تعداد ردیف'})

file_name = PROJECT_PATH + "Report/Region_District_Peyman_Address.xlsx"
tmp.to_excel(file_name, index=False)

print(tmp)
del tmpG, tmp, file_name


# %% Report: Species Plant Class In Data
tmp = data.groupby(['Ghalam_Type', 'Ghalam_SubType']).size()
tmp = tmp.reset_index()
tmp = tmp.rename(columns={'Ghalam_Type': 'نوع قلم',
                          'Ghalam_SubType': 'زیرمجموعه هر قلم',
                          0: 'تعداد ردیف'})

file_name = PROJECT_PATH + '/Report/Ghalam_SubGhalam.xlsx'
tmp.to_excel(file_name, index=False)

del tmp, file_name

tmp = data.groupby(['Ghalam_SubType', 'Ghalam_Type']).size()
tmp = tmp.reset_index()
tmp = tmp.rename(columns={'Ghalam_Type': 'نوع قلم',
                          'Ghalam_SubType': 'زیرمجموعه هر قلم',
                          0: 'تعداد ردیف'})

file_name = PROJECT_PATH + '/Report/SubGhalam_Ghalam.xlsx'
tmp.to_excel(file_name, index=False)

del tmp, file_name


# %% Report: Irrigation - Species - RDPA
tmpG = data.groupby(
    ['Region', 'District', 'Peyman', 'Address', 'Irrigation', 'Species']
)

tmp = tmpG.agg({
    'Irrigation_Area': 'sum',
    'Address_Area': 'mean',
    'Peyman_Area': 'mean'
})

file_name = PROJECT_PATH + "Report/Report_RDPAIS_Irrigated_Address_Peyman_Area.xlsx"

tmp.to_excel(file_name, index=True)

del tmpG, tmp, file_name


# %% Report: Irrigation - Species - RDP
tmpG = data.groupby(
    ['Region', 'District', 'Peyman', 'Irrigation', 'Species']
)

tmp = tmpG.agg({
    'Irrigation_Area': 'sum'
})

file_name = PROJECT_PATH + "Report/Report_RDPIS_Irrigated_Area.xlsx"

tmp.to_excel(file_name, index=True)

del tmpG, tmp, file_name


# %% Report: Species - Irrigation - RDP
tmpG = data.groupby(
    ['Region', 'District', 'Peyman', 'Species', 'Irrigation']
)

tmp = tmpG.agg({
    'Irrigation_Area': 'sum'
})

file_name = PROJECT_PATH + "Report/Report_RDPSI_Irrigated_Area.xlsx"

tmp.to_excel(file_name, index=True)

del tmpG, tmp, file_name


# %% Report: Irrigation - Species - RD
tmpG = data.groupby(
    ['Region', 'District', 'Irrigation', 'Species']
)

tmp = tmpG.agg({
    'Irrigation_Area': 'sum'
})

file_name = PROJECT_PATH + "Report/Report_RDIS_Irrigated_Area.xlsx"

tmp.to_excel(file_name, index=True)

del tmpG, tmp, file_name


# %% Report:  Species - Irrigation - RD
tmpG = data.groupby(
    ['Region', 'District', 'Species', 'Irrigation']
)

tmp = tmpG.agg({
    'Irrigation_Area': 'sum'
})

file_name = PROJECT_PATH + "Report/Report_RDSI_Irrigated_Area.xlsx"

tmp.to_excel(file_name, index=True)

del tmpG, tmp, file_name


# %% Report: Irrigation - Species - R
tmpG = data.groupby(
    ['Region', 'Irrigation', 'Species']
)

tmp = tmpG.agg({
    'Irrigation_Area': 'sum'
})

file_name = PROJECT_PATH + "Report/Report_RIS_Irrigated_Area.xlsx"

tmp.to_excel(file_name, index=True)

del tmpG, tmp, file_name


# %% Report: Irrigation - Species - R
tmpG = data.groupby(
    ['Region', 'Species', 'Irrigation']
)

tmp = tmpG.agg({
    'Irrigation_Area': 'sum'
})

file_name = PROJECT_PATH + "Report/Report_RSI_Irrigated_Area.xlsx"

tmp.to_excel(file_name, index=True)

del tmpG, tmp, file_name


# %% Report: Irrigation - Species
tmp = data.pivot_table(values="Irrigation_Area",
                       index=['Species'],
                       columns=['Irrigation'],
                       aggfunc=['count', 'sum'],
                       margins=True,
                       margins_name="Total")

file_name = PROJECT_PATH + "Report/PivotTable_Species_Irrigation_Count_Area.xlsx"
tmp.to_excel(file_name)


# %% Report: Irrigation - Species - Region
tmp = data.pivot_table(values="Irrigation_Area",
                       index=['Region', 'Species'],
                       columns=['Irrigation'],
                       aggfunc=['count', 'sum'],
                       margins=True,
                       margins_name="Total")

file_name = PROJECT_PATH + "Report/PivotTable_Region_Species_Irrigation_Count_Area.xlsx"
tmp.to_excel(file_name)


# %% Report: Irrigation - Species - Region - District
tmp = data.pivot_table(values="Irrigation_Area",
                       index=['Region', 'District', 'Species'],
                       columns=['Irrigation'],
                       aggfunc=['count', 'sum'],
                       margins=True,
                       margins_name="Total")

file_name = PROJECT_PATH + \
    "Report/PivotTable_Region_District_Species_Irrigation_Count_Area.xlsx"
tmp.to_excel(file_name)


# %% Check Peyman Area
tmpG = data.groupby(['Region', 'District', 'Peyman'])

tmp = tmpG.agg({
    'Peyman_Area': ['min', 'max']
}).reset_index()

tmp.columns = ['Region', 'District', 'Peyman',
               'Peyman_Area_min', 'Peyman_Area_max']

tmp = tmp >> define(Check=if_else('Peyman_Area_min == Peyman_Area_max',
                                  True,
                                  False))

tmp.to_excel(PROJECT_PATH + "Report/Peyman_Area_Check.xlsx", index=False)


# %% ETL Calculation - Efficacy Class
Eff = {
    '01': 0.70,
    '02': 0.60,
    '03': 0.80,
    '04': 0.80
}

data['Eff'] = list(map(Eff.get, data['Irrigation']))


# %% ETL Calculation - Microclimate Class
Kmc = {
    '01': 1.20,
    '02': 1.20,
    '03': 1.15,
    '04': 1.10,
    '05': 1.10,
    '06': 1.05,
    '07': 1.05,
    '08': 1.00,
    '09': 0.90,
    '10': 0.80,
    '11': 1.00,
    '12': 1.05,
    '13': 1.05
}

data['Kmc'] = list(map(Kmc.get, data['Spot']))


# %% ETL Calculation - Species Plant Class
Ksp = {
    '01': 0.70,
    '02': 0.75,
    '04': 0.80,
    '05': 0.80,
    '06': 0.80,
    '10': 0.70,
    '12': 0.49,
    '13': 0.00,
    '14': 0.80
}

data['Ksp'] = list(map(Ksp.get, data['Species']))


# %% ETL Calculation - Change Species Plant Class In Year (1)
KspChangeYear = {
    '01': [0.60, 0.75, 0.90, 1.00, 0.95, 0.80, 0.60, 0.45, 0.35, 0.25, 0.30, 0.40],
    '02': [0.60, 0.75, 0.90, 1.00, 0.95, 0.80, 0.60, 0.45, 0.35, 0.25, 0.30, 0.40],
    '04': [0.60, 0.75, 0.90, 1.00, 0.95, 0.80, 0.60, 0.45, 0.35, 0.25, 0.30, 0.40],
    '05': [0.60, 0.75, 0.90, 1.00, 0.95, 0.80, 0.60, 0.45, 0.35, 0.25, 0.30, 0.40],
    '06': [0.60, 0.75, 0.90, 1.00, 0.95, 0.80, 0.60, 0.45, 0.35, 0.25, 0.30, 0.40],
    '10': [0.60, 0.75, 0.90, 1.00, 0.95, 0.80, 0.60, 0.45, 0.35, 0.25, 0.30, 0.40],
    '12': [0.60, 0.75, 0.90, 1.00, 0.95, 0.80, 0.60, 0.45, 0.35, 0.25, 0.30, 0.40],
    '13': [0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00, 0.00],
    '14': [0.60, 0.75, 0.90, 1.00, 0.95, 0.80, 0.60, 0.45, 0.35, 0.25, 0.30, 0.40]
}

Ksp_Month_Name = ['Ksp_M01', 'Ksp_M02', 'Ksp_M03', 'Ksp_M04', 'Ksp_M05', 'Ksp_M06',
                  'Ksp_M07', 'Ksp_M08', 'Ksp_M09', 'Ksp_M10', 'Ksp_M11', 'Ksp_M12']

KspChangeYear = pd.DataFrame(KspChangeYear, index=Ksp_Month_Name).T

KspChangeYear = KspChangeYear.reset_index()
KspChangeYear = KspChangeYear.rename(columns={'index': 'Species'})

data = data >> left_join(KspChangeYear, on='Species')


# %% ETL Calculation - Change Species Plant Class In Year (2)
tmpG = data.groupby(by=['Region', 'District', 'Peyman',
                        'Address', 'Irrigation'])['Irrigation_Area']

tmp = tmpG.sum().reset_index().rename(
    columns={'Irrigation_Area': 'SubIrrigation_Area'}
)

data = data >> left_join(tmp,
                         on=['Region', 'District', 'Peyman', 'Address', 'Irrigation'])

data = data >> define(
    Ksp_M01='Ksp_M01 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M02='Ksp_M02 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M03='Ksp_M03 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M04='Ksp_M04 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M05='Ksp_M05 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M06='Ksp_M06 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M07='Ksp_M07 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M08='Ksp_M08 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M09='Ksp_M09 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M10='Ksp_M10 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M11='Ksp_M11 * Ksp * Irrigation_Area / SubIrrigation_Area')
data = data >> define(
    Ksp_M12='Ksp_M12 * Ksp * Irrigation_Area / SubIrrigation_Area')

tmpG = data.groupby(
    by=['Region', 'District', 'Peyman', 'Address', 'Irrigation'])

tmpG = tmpG['Ksp_M01', 'Ksp_M02', 'Ksp_M03', 'Ksp_M04', 'Ksp_M05', 'Ksp_M06',
            'Ksp_M07', 'Ksp_M08', 'Ksp_M09', 'Ksp_M10', 'Ksp_M11', 'Ksp_M12']

tmp = tmpG.sum().reset_index()

tmp = tmp.rename(columns={
    'Ksp_M01': 'Ksp_M01_Sum_SubIrigation',
    'Ksp_M02': 'Ksp_M02_Sum_SubIrigation',
    'Ksp_M03': 'Ksp_M03_Sum_SubIrigation',
    'Ksp_M04': 'Ksp_M04_Sum_SubIrigation',
    'Ksp_M05': 'Ksp_M05_Sum_SubIrigation',
    'Ksp_M06': 'Ksp_M06_Sum_SubIrigation',
    'Ksp_M07': 'Ksp_M07_Sum_SubIrigation',
    'Ksp_M08': 'Ksp_M08_Sum_SubIrigation',
    'Ksp_M09': 'Ksp_M09_Sum_SubIrigation',
    'Ksp_M10': 'Ksp_M10_Sum_SubIrigation',
    'Ksp_M11': 'Ksp_M11_Sum_SubIrigation',
    'Ksp_M12': 'Ksp_M12_Sum_SubIrigation'
})

data = data >> left_join(tmp,
                         on=['Region', 'District', 'Peyman', 'Address', 'Irrigation'])


# %% ETL Calculation - Density Plant Class
Kd = {
    1: 1,
    2: 1,
    3: 1.2,
    4: 1.2,
    5: 1.3,
    6: 1.3,
    7: 1.3,
    8: 1.3
}


def name(x):
    x = list(x)
    if "10" in x and "13" in x:
        return -1
    else:
        return 0


tmpAG = data.groupby(
    by=['Region', 'District', 'Peyman', 'Address', 'Irrigation'])
tmpA = tmpAG.size().reset_index().rename(columns={0: 'Species_Count'})

tmpBG = data.groupby(by=['Region', 'District', 'Peyman', 'Address', 'Extera'])
tmpB = tmpBG.size().reset_index().rename(columns={0: 'Asleh'})

tmpC = tmpA >> left_join(tmpB, on=['Region', 'District', 'Peyman', 'Address'])
tmpC = tmpC >> call(pd.DataFrame.fillna, 0)

tmpDG = data.groupby(by=['Region', 'District', 'Peyman', 'Address'])
tmpD = tmpDG.agg({'Species': name})
tmpD = tmpD.reset_index().rename(columns={'Species': 'Double_Asleh'})

tmpE = tmpC >> left_join(tmpD, on=['Region', 'District', 'Peyman', 'Address'])
tmpE = tmpE >> define(Species_Count='Species_Count + Asleh + Double_Asleh')
tmpE = tmpE >> select('Extera', 'Asleh', 'Double_Asleh', drop=True)

data = data >> left_join(tmpE,
                         on=['Region', 'District', 'Peyman', 'Address', 'Irrigation'])

data['Kd'] = list(map(Kd.get, data['Species_Count']))


# %% ETL Calculation - KL Calculate
data = data >> define(KL_M01='Ksp_M01_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M02='Ksp_M02_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M03='Ksp_M03_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M04='Ksp_M04_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M05='Ksp_M05_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M06='Ksp_M06_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M07='Ksp_M07_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M08='Ksp_M08_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M09='Ksp_M09_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M10='Ksp_M10_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M11='Ksp_M11_Sum_SubIrigation * Kmc * Kd')
data = data >> define(KL_M12='Ksp_M12_Sum_SubIrigation * Kmc * Kd')


# %% ETL Calculation

del tmpG, tmp, final_result

ETP = {
    'Dry': [103.4, 151.3, 196.2, 219.3, 199.0, 157.9, 109.0, 63.8, 38.7, 27.8, 32.7, 60.7],
    'Normal': [116.1, 168.9, 206.6, 231.3, 201.4, 172, 104.2, 56.9, 38, 39.8, 49.6, 81.1],
    'Wet': [122.6, 176.6, 220.8, 255.8, 236.9, 185.8, 123.7, 64.2, 41.9, 35.3, 42.0, 79.9]
}

Prec = {
    'Dry': [20.6, 14.1, 4.8, 0.5, 0.08, 0.3, 2.7, 6.2, 9.4, 13.2, 15.8, 22.9],
    'Normal': [25.8, 17.6, 6.0, 0.6, 0.1, 0.4, 2.7, 7.8, 11.8, 16.5, 19.7, 28.7],
    'Wet': [31.0, 21.1, 7.2, 0.7, 0.1, 0.5, 3.2, 9.4, 14.2, 19.8, 23.6, 34.4]
}

for i in ['Dry', 'Normal', 'Wet']:

    ETL_Info = {
        'Number_Day': [31, 31, 31, 31, 31, 31, 30, 30, 30, 30, 30, 29],
        'PET': ETP[i],
        'Eff_Precipitation': Prec[i],
        'Ground_Water': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        'Deficit_Irrigation': [0.95, 0.95, 0.95, 0.95, 0.95, 0.95, 0.95, 0.95, 0.95, 0.95, 0.95, 0.95],
        'LF': [0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05, 0.05],
        'Shading_Area': [1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00],
        'Percentage_Wetted_Area': [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100]
    }

    Month_Name = ['M01', 'M02', 'M03', 'M04', 'M05', 'M06',
                  'M07', 'M08', 'M09', 'M10', 'M11', 'M12']

    ETL_Info = pd.DataFrame(ETL_Info, index=Month_Name)

    tmpG = data.groupby(
        by=['Region', 'District', 'Peyman', 'Address', 'Irrigation']
    )

    tmpG = tmpG['KL_M01', 'KL_M02', 'KL_M03', 'KL_M04', 'KL_M05', 'KL_M06',
                'KL_M07', 'KL_M08', 'KL_M09', 'KL_M10', 'KL_M11', 'KL_M12',
                'SubIrrigation_Area', 'Eff']

    tmp = tmpG.mean().reset_index()

    month = "M01"
    tmp = tmp >> define(ETL_M01='(KL_M01 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M01=if_else('ETL_M01 < 0', 0, 'ETL_M01'))
    tmp = tmp >> define(
        IrriReq_M01='ETL_M01 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M02"
    tmp = tmp >> define(ETL_M02='(KL_M02 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M02=if_else('ETL_M02 < 0', 0, 'ETL_M02'))
    tmp = tmp >> define(
        IrriReq_M02='ETL_M02 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M03"
    tmp = tmp >> define(ETL_M03='(KL_M03 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M03=if_else('ETL_M03 < 0', 0, 'ETL_M03'))
    tmp = tmp >> define(
        IrriReq_M03='ETL_M03 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M04"
    tmp = tmp >> define(ETL_M04='(KL_M04 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M04=if_else('ETL_M04 < 0', 0, 'ETL_M04'))
    tmp = tmp >> define(
        IrriReq_M04='ETL_M04 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M05"
    tmp = tmp >> define(ETL_M05='(KL_M05 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M05=if_else('ETL_M05 < 0', 0, 'ETL_M05'))
    tmp = tmp >> define(
        IrriReq_M05='ETL_M05 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M06"
    tmp = tmp >> define(ETL_M06='(KL_M06 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M06=if_else('ETL_M06 < 0', 0, 'ETL_M06'))
    tmp = tmp >> define(
        IrriReq_M06='ETL_M06 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M07"
    tmp = tmp >> define(ETL_M07='(KL_M07 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M07=if_else('ETL_M07 < 0', 0, 'ETL_M07'))
    tmp = tmp >> define(
        IrriReq_M07='ETL_M07 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M08"
    tmp = tmp >> define(ETL_M08='(KL_M08 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M08=if_else('ETL_M08 < 0', 0, 'ETL_M08'))
    tmp = tmp >> define(
        IrriReq_M08='ETL_M08 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M09"
    tmp = tmp >> define(ETL_M09='(KL_M09 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M09=if_else('ETL_M09 < 0', 0, 'ETL_M09'))
    tmp = tmp >> define(
        IrriReq_M09='ETL_M09 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M10"
    tmp = tmp >> define(ETL_M10='(KL_M10 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M10=if_else('ETL_M10 < 0', 0, 'ETL_M10'))
    tmp = tmp >> define(
        IrriReq_M10='ETL_M10 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M11"
    tmp = tmp >> define(ETL_M11='(KL_M11 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M11=if_else('ETL_M11 < 0', 0, 'ETL_M11'))
    tmp = tmp >> define(
        IrriReq_M11='ETL_M11 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    month = "M12"
    tmp = tmp >> define(ETL_M12='(KL_M12 * ETL_Info.loc[month]["PET"] * ETL_Info.loc[month]["Deficit_Irrigation"] * ETL_Info.loc[month]["Shading_Area"] - (ETL_Info.loc[month]["Eff_Precipitation"] + ETL_Info.loc[month]["Ground_Water"]) * ETL_Info.loc[month]["Percentage_Wetted_Area"] / 100) / ETL_Info.loc[month]["Number_Day"] / (1 - ETL_Info.loc[month]["LF"]) / Eff')
    tmp = tmp >> define(ETL_M12=if_else('ETL_M12 < 0', 0, 'ETL_M12'))
    tmp = tmp >> define(
        IrriReq_M12='ETL_M12 * ETL_Info.loc[month]["Number_Day"] * SubIrrigation_Area / 1000 / 1000'
    )

    tmp = tmp >> define(ETL_YEAR='ETL_M01 * ETL_Info.loc["M01"]["Number_Day"] + ETL_M02 * ETL_Info.loc["M02"]["Number_Day"] + ETL_M03 * ETL_Info.loc["M03"]["Number_Day"] + ETL_M04 * ETL_Info.loc["M04"]["Number_Day"] + ETL_M05 * ETL_Info.loc["M05"]["Number_Day"] + ETL_M06 * ETL_Info.loc["M06"]["Number_Day"] + ETL_M07 * ETL_Info.loc["M07"]["Number_Day"] + ETL_M08 * ETL_Info.loc["M08"]["Number_Day"] + ETL_M09 * ETL_Info.loc["M09"]["Number_Day"] + ETL_M10 * ETL_Info.loc["M10"]["Number_Day"] + ETL_M11 * ETL_Info.loc["M11"]["Number_Day"] + ETL_M12 * ETL_Info.loc["M12"]["Number_Day"]')
    tmp = tmp >> define(
        IrriReq_YEAR='ETL_YEAR * SubIrrigation_Area / 1000 / 1000'
    )

    tmp = tmp >> select(
        'Region', 'District', 'Peyman', 'Address', 'Irrigation', 'SubIrrigation_Area',
        'ETL_M01', 'ETL_M02', 'ETL_M03',
        'ETL_M04', 'ETL_M05', 'ETL_M06',
        'ETL_M07', 'ETL_M08', 'ETL_M09',
        'ETL_M10', 'ETL_M11', 'ETL_M12',
        'IrriReq_M01', 'IrriReq_M02', 'IrriReq_M03',
        'IrriReq_M04', 'IrriReq_M05', 'IrriReq_M06',
        'IrriReq_M07', 'IrriReq_M08', 'IrriReq_M09',
        'IrriReq_M10', 'IrriReq_M11', 'IrriReq_M12',
        'ETL_YEAR', 'IrriReq_YEAR'
    )

    tmp.columns = ['Region', 'District', 'Peyman', 'Address',
                   'Irrigation', 'SubIrrigation_Area'] + list(i + '_' + tmp.columns[6:])

    try:
        final_result
    except NameError:
        final_result = tmp
    else:
        tmp = tmp >> select('-SubIrrigation_Area')
        final_result = pd.merge(final_result,
                                tmp,
                                how='left',
                                on=['Region', 'District', 'Peyman', 'Address', 'Irrigation'])

final_result.to_excel(PROJECT_PATH + "Report/Final_Result.xlsx",
                      index=False)


# %% Load Shapefiles
region_shp = PROJECT_PATH + 'Data/Mashhad_City_Layers/Region/ShapeFile/Regions.shp'
district_shp = PROJECT_PATH + \
    'Data/Mashhad_City_Layers/District/ShapeFile/Districts.shp'

region_df = gpd.read_file(filename=region_shp)[['Name', 'geometry']]
district_df = gpd.read_file(filename=district_shp)[['Name', 'geometry']]

region_df.columns = ['ID', 'geometry']
district_df.columns = ['ID', 'geometry']


# %% Report And Plot ETL
tmpData = final_result >> select(
    'Region', 'District', 'Peyman', 'Address', 'Irrigation', 'SubIrrigation_Area',
    endswith='_YEAR'
)

tmpG = tmpData.groupby(['Region'])

tmp = tmpG.agg({'SubIrrigation_Area': 'sum',
                'Dry_ETL_YEAR': 'mean',
                'Normal_ETL_YEAR': 'mean',
                'Wet_ETL_YEAR': 'mean',
                'Dry_IrriReq_YEAR': 'sum',
                'Normal_IrriReq_YEAR': 'sum',
                'Wet_IrriReq_YEAR': 'sum'})

tmp = tmp.reset_index().round(1)

file_name = PROJECT_PATH + "Report/Report_ETL_Region.xlsx"
tmp.to_excel(file_name, index=False)

tmp['ID'] = 'Region' + ' ' + tmp['Region']

tmp = tmp.merge(region_df,
                how='right',
                on='ID')

print(tmp)


tmp_json = json.loads(tmp.to_json())

tmp_json = json.dumps(tmp_json)

geosource = GeoJSONDataSource(geojson=tmp_json)

palette = brewer['YlGnBu'][7]

palette = palette[::-1]

color_mapper = LinearColorMapper(palette=palette,
                                 low=0,
                                 high=1400)

tick_labels = {'0': '0',
               '200': '200',
               '400': '400',
               '600': '600',
               '800': '800',
               '1000': '1000',
               '1200': '1200',
               '1400': '> 1400'}

hover = HoverTool(tooltips=[('منطقه', '@Region'),
                            ('مساحت تحت آبیاری بر حسب متر مربع',
                             '@SubIrrigation_Area'),
                            ('نیاز آبیاری بر حسب میلیمتر در سال', '@Normal_ETL_YEAR'),
                            ('حجم آبیاری بر حسب میلیون متر مکعب', '@Normal_IrriReq_YEAR')])

color_bar = ColorBar(color_mapper=color_mapper,
                     label_standoff=8,
                     width=400,
                     height=20,
                     border_line_color=None,
                     location=(0, 0),
                     orientation='horizontal',
                     major_label_overrides=tick_labels)

p = figure(title='مشهد',
           toolbar_location=None,
           tools=[hover])

p.xgrid.grid_line_color = None
p.ygrid.grid_line_color = None

p.patches('xs',
          'ys',
          source=geosource,
          fill_color={'field': 'Normal_IrriReq_YEAR',
                      'transform': color_mapper},
          line_color='black',
          line_width=0.25,
          fill_alpha=1)

p.add_layout(color_bar,
             'below')

output_notebook()

show(p)


# %% Plot
