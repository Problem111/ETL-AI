import os
import pandas as pd

CURR_DIR_PATH = os.path.dirname(os.path.abspath(__file__))

course_path = CURR_DIR_PATH + "/data"
dir_path = CURR_DIR_PATH + "/data/dataframes"

# 3.1.1 E

# Read, and concatenate alla similar files in the directory

def extract(dir_path):
    # Get a list of all files in the directory
    files = os.listdir(dir_path)

    # Initialize an empty dataframe with column names to hold the concatenated data
    data = {}
    prefix_check = ""

    for file in files:

        if file.endswith(".csv") or file.endswith(".txt"):
            prefix = file.split("_")[0]

            # Check if this prefix is different from the previous one
            if prefix != prefix_check:
                # If this prefix is different, concatenate all dataframes with the previous prefix and save to file
                if prefix_check in data.keys():
                    data_df = pd.concat(data[prefix_check])
                    data_df.to_csv(os.path.join(dir_path + "/dataframes", prefix_check + ".csv"), index=False)
                    data.pop(prefix_check)

                # Update prefix_check and create a new key in the data dictionary
                prefix_check = prefix
                data[prefix_check] = []

            # Read in the file and append it to the list in the data dictionary
            data[prefix_check].append(pd.read_csv(os.path.join(dir_path, file)))

    # Concatenate all dataframes with the last prefix encountered and save to file
    if prefix_check in data.keys():
        data_df = pd.concat(data[prefix_check])
        data_df.to_csv(os.path.join(dir_path + "/dataframes", prefix_check + ".csv"), index=False)
        data.pop(prefix_check)


def transform(dir_path):

    for file in os.listdir(dir_path):
            df = pd.read_csv(dir_path + "/" + file)
            if 'firstname' in df.columns and 'surname' in df.columns:
                df['name'] = df['firstname'] + ' ' + df['surname']
                df.drop(['firstname', 'surname'], axis=1, inplace=True)
                df.to_csv(dir_path + "/" + file, index=False)


    for file in os.listdir(dir_path):
        df = pd.read_csv(dir_path + "/" + file)
        if 'attendance' in df.columns:
            df['Late'] = ''
            df.loc[df['attendance'] < 60, 'Late'] = 'late'
            df.to_csv(dir_path + "/" + file, index=False)


    for file in os.listdir(dir_path):
        df = pd.read_csv(dir_path + "/" + file)

        if 'attendance' in df.columns:
            df1 = df['attendance']
            df['late duration'] = 0
            for i in df1:

                if i < 60:
                    df.loc[df['attendance'] == i, 'late duration'] = 60 - i
                    df.to_csv(dir_path + "/" + file, index=False)


def load(dir_path):

    for file in os.listdir(dir_path):
        df = pd.read_csv(dir_path + "/" + file)
        if 'late duration' in df.columns:
            new_df = df.loc[df["late duration"] >= 1, ["name", "late duration"]]
            new_df.to_csv(dir_path + "/lazy_students.csv", index=False)

    lazy_students_list = []

    for file in os.listdir(dir_path):
        df = pd.read_csv(dir_path + "/" + file)
        if 'late duration' in df.columns:
            new_df = df.loc[df["late duration"] >= 1, ["name", "late duration"]]
            lazy_students_list.append(new_df)

    lazy_students_df = pd.concat(lazy_students_list, ignore_index=True)
    lazy_students_df.to_csv(dir_path + "/lazy_students.csv", index=False)

    df = pd.read_csv(dir_path + '/lazy_students.csv')

    # Group by the "name" column and sum the "late duration" column for each group
    final_csv = df.groupby('name', as_index=False).agg({'late duration': 'sum'})
    final_csv.sort_values(by='late duration', ascending=False)
    final_csv.to_csv(dir_path + '/lazy_students.csv', index=False)


extract(course_path)
transform(dir_path)
load(dir_path)