
# Author: Matthew Drew
# Last updated: 3/27/2023

# CSV
# ObjectID = unique id per object
# HostID = unique id of the parent object. A door would have this the HostID's value as the wall's ObjectID
# ImageX_Object_ID contain the imageIds linking them back to the JSON.
# disregard rows without ObjectID or HostID

# JSON
# some objects returned contain imageIds, which contains the imageIds to link them to the CSV file

# OBJECTIVE:
# update the JSON with the correct HostID that corresponds to it. Name that key 'parent_id' in the JSON file. Save all updated JSONs.

import json
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def associate_parent_ids(path):
    """ Given a relative path of a JSON file, for each item in the 'ops_3d' key, associate the parent ID based on the predefined .csv file (EXP_ObjectID_HostID.csv)
    
    Parameters
    ----------
    path: str
        The relative path of the JSON file
    
    Returns
    ------
    pandas DataFrame
        the contents of the json file, as a pandas dataframe, with the new 'parent_id' column
    """

    # read json file
    json_contents = read_json_from_file(path)

    # get a pandas dataframe from the json file we just read, but only below the 'ops_3d' key, since that's all we care about
    df = pd.DataFrame.from_dict(json_contents['ops_3d'])

    csv_file = pd.read_csv('EXP_ObjectID_HostID.csv', encoding='utf-16', sep='\t')

    csv_file = csv_file.dropna(subset=['Image1_Object_ID'])
    csv_file = csv_file.dropna(subset=['Image2_Object_ID'])
    csv_file = csv_file.dropna(subset=['Image3_Object_ID'])

    csv_file['Image1_Object_ID'] = csv_file['Image1_Object_ID'].astype(int)
    csv_file['Image2_Object_ID'] = csv_file['Image2_Object_ID'].astype(int)
    csv_file['Image3_Object_ID'] = csv_file['Image3_Object_ID'].astype(int)

    # create a list of lists that associate the Host_ID, Object_ID, and ImageX_Object_ID columns together
    associated_img_obj_id_list = []
    for key, row in csv_file.iterrows():
        associated_img_obj_id_list.append([row['Host_ID'], row['Object_ID'], int(row['Image1_Object_ID']), int(row['Image2_Object_ID']), int(row['Image3_Object_ID'])])

    for row in associated_img_obj_id_list:
        for i, imageId_list in enumerate(df['imageIds'].to_numpy()): # i = index, imageId_list = list of imageIds
            # set the imageIds to integers
            imageId_list = [int(i) for i in imageId_list]
            result_set = set(imageId_list).intersection(row)
            if(len(result_set) > 0):
                host_id = row[0] # get the HostID
                
                # now, we must find the ImageX_Object_IDs for the Host_IDs. There are values of Object_ID == Host_ID.
                # let's do this by reiterating through the associated_img_obj_id_list and seeing if they're equal

                for obj_id_list in associated_img_obj_id_list:
                    associated_obj_id = []
                    if(host_id == obj_id_list[1]):
                        associated_obj_id.append(obj_id_list[2])
                        associated_obj_id.append(obj_id_list[3])
                        associated_obj_id.append(obj_id_list[4])

                        # reiterate through the json file, check for the matching imageIDs
                        for i_x, imageId_list in enumerate(df['imageIds'].to_numpy()):
                            imageId_list = [int(i) for i in imageId_list]
                            result_set = set(imageId_list).intersection(associated_obj_id)

                            # if there's a match
                            if(len(result_set) > 0):
                                # get the unique_id of the parent
                                unique_id = df.at[i_x, 'unique_id']

                                # set the parent's unique_id to the row in question's parent_id row
                                df.at[i, 'parent_id'] = unique_id
    # replace the ops_3d key with the df that was created
    df = df.fillna('')
    json_contents['ops_3d'] = df.to_dict(orient='records')
    
    return json_contents

def read_json_from_file(path):
    """ Given a relative path of a JSON file, return the JSON object
    
    Parameters
    ----------
    path: str
        The relative path of the JSON file
    
    Returns
    ------
    dict
        dict continuing the contents of the JSON file
    """
    with open(path, 'r') as f:
        return json.load(f)

def write_df_to_json_file(df, path):
    """ Given a pandas dataframe, write to a given path.
    
    Parameters
    ----------
    df: pandas DataFrame
        The dataframe object that contains the data to be written to the path
    path: str
        The relative path of the JSON file
    """
    with open(path, 'w') as f:
        json.dump(df, f, indent=4)


result_json = associate_parent_ids('3d3fde25-fc47-47ad-bda4-0b438196045b.json')
write_df_to_json_file(result_json, 'output/1.json')

result_json = associate_parent_ids('763fdd40-9408-45bb-b532-3f90b5c7c5d1.json')
write_df_to_json_file(result_json, 'output/2.json')

result_json = associate_parent_ids('b73070b3-7625-4975-872a-967b2297a458.json')
write_df_to_json_file(result_json, 'output/3.json')






