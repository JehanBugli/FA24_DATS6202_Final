"""
This script gathers desired data from the American Community Survey API for census "block groups".
"""

# %%

import requests
import csv
import codecs
# %%
code_dict = {
    "B01003_001E": "total_pop",
    "B17001_002E": "below_poverty_level",
    "B01001_003E": "male_0_4",
    "B01001_004E": "male_5_9",
    "B01001_005E": "male_10_14",
    "B01001_006E": "male_15_17",
    "B01001_007E": "male_18_19",
    "B01001_008E": "male_20",
    "B01001_009E": "male_21",
    "B01001_010E": "male_22_24",
    "B01001_011E": "male_25_29",
    "B01001_012E": "male_30_34",
    "B01001_013E": "male_35_39",
    "B01001_014E": "male_40_44",
    "B01001_015E": "male_45_49",
    "B01001_016E": "male_50_54",
    "B01001_017E": "male_55_59",
    "B01001_018E": "male_60_61",
    "B01001_019E": "male_62_64",
    "B01001_020E": "male_65_66",
    "B01001_021E": "male_67_69",
    "B01001_022E": "male_70_74",
    "B01001_023E": "male_75_79",
    "B01001_024E": "male_80_84",
    "B01001_025E": "male_85+",
    "B01001_027E": "female_0_4",
    "B01001_028E": "female_5_9",
    "B01001_029E": "female_10_14",
    "B01001_030E": "female_15_17",
    "B01001_031E": "female_18_19",
    "B01001_032E": "female_20",
    "B01001_033E": "female_21",
    "B01001_034E": "female_22_24",
    "B01001_035E": "female_25_29",
    "B01001_036E": "female_30_34",
    "B01001_037E": "female_35_39",
    "B01001_038E": "female_40_44",
    "B01001_039E": "female_45_49",
    "B01001_040E": "female_50_54",
    "B01001_041E": "female_55_59",
    "B01001_042E": "female_60_61",
    "B01001_043E": "female_62_64",
    "B01001_044E": "female_65_66",
    "B01001_045E": "female_67_69",
    "B01001_046E": "female_70_74",
    "B01001_047E": "female_75_79",
    "B01001_048E": "female_80_84",
    "B01001_049E": "female_85+",
    "B02001_002E": "race_white",
    "B02001_003E": "race_black",
    "B02001_004E": "race_native",
    "B02001_005E": "race_asian",
    "B02001_007E": "race_other",
    "B05012_002E": "native",
    "B05012_003E": "foreign_born",
    "B08301_001E": "trans_total",
    "B08301_002E": "trans_car",
    "B08301_003E": "trans_car_alone",
    "B08301_004E": "trans_carpool",
    "B08301_010E": "trans_public",
    "B08301_011E": "trans_bus",
    "B08301_012E": "trans_subway",
    "B08301_013E": "trans_train",
    "B08301_014E": "trans_light_rail",
    "B08301_015E": "trans_ferry",
    "B08301_016E": "trans_taxi",
    "B08301_017E": "trans_motorcycle",
    "B08301_019E": "trans_walk",
    "B15003_017E": "edu_hs_diploma",
    "B15003_018E": "edu_ged",
    "B15003_019E": "edu_some_col1",
    "B15003_020E": "edu_some_col2",
    "B15003_021E": "edu_assoc",
    "B15003_022E": "edu_bach",
    "B15003_023E": "edu_mast",
    "B15003_024E": "edu_prof",
    "B15003_025E": "edu_phd",
    "B19122_001E": "fam_earners_total",
    "B19122_002E": "fam_earners_0",
    "B19122_003E": "fam_earners_1",
    "B19122_004E": "fam_earners_2",
    "B19122_005E": "fam_earners_3+",
    "B25001_001E": "housing_units",
    "B25035_001E": "med_year_built",
    "B21001_002E": "vet_veteran",
    "B21001_003E": "vet_nonveteran",
    "B12007_001E": "mar_median_age_m",
    "B12007_002E": "mar_median_age_f"
}

# %%

code_string = ','.join(
    code_dict.keys()
)

# %% 

def process_query(url):

    query = requests.get(url)

    if query.status_code == 200:
        return query.json()
    else:
        raise Exception(f"\nRequest Error: \n{query.status_code}\n\nURL:\n{url}")


# %%

states_url = 'https://api.census.gov/data/2022/acs/acs5?get=NAME&for=state:*'

states_list = process_query(states_url)

# %%

state_dict = {}

state_headers = states_list[0]

for state in states_list[1:]:
    state_dict[state[1]] = state[0]
# %%

print(state_dict)

# %%

all_data = []

# %%

def get_state_data(state_code='*', first_half = True):

    code_list_trunc =  list(code_dict.keys())[:48] if first_half else list(code_dict.keys())[48:]

    code_string_trunc = ','.join(code_list_trunc)

    url = '&'.join([
        f'https://api.census.gov/data/2022/acs/acs5?get={code_string_trunc}',
        'for=block%20group:*',
        f'in=state:{state_code}%20county:*'
    ])

    return process_query(url)

# %%

def list_to_dicts(state_data1, state_data2):

    output = []

    headers1 = state_data1[0]
    headers2 = state_data2[0]

    for i in range(1, len(state_data1)):

        row_dict = {}

        row1 = state_data1[i]
        row2 = state_data2[i]

        for i, r in enumerate(row1):

            header = headers1[i] if headers1[i] not in code_dict else code_dict[headers1[i]]

            row_dict[header] = r
        
        for i, r in enumerate(row2):

            header = headers2[i] if headers2[i] not in code_dict else code_dict[headers2[i]]

            if headers2[i] not in row_dict:

                row_dict[header] = r

        output.append(row_dict)

    return output

# %%

i = 0

for state_code, state_name in state_dict.items():

    i += 1

    if i > 3:
        break

    print(state_name)

    state_data_1 = get_state_data(state_code, True)
    state_data_2 = get_state_data(state_code, False)
    dicts = list_to_dicts(state_data_1, state_data_2)
    all_data.extend(dicts)

# %%

print(all_data[0].keys())

# %%
with codecs.open('census_data.csv', 'w', encoding='utf-8-sig') as file:
    dict_writer = csv.DictWriter(file, all_data[0].keys())
    dict_writer.writeheader()
    dict_writer.writerows(all_data)

# %%