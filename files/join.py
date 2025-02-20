import pandas as pd
import glob

def get_header_count(file_path):
    count = 0
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split(',')
            if (parts[1] != ""):
                try:
                    float(parts[1])
                    break
                except (ValueError, IndexError):
                    count += 1
    return count

def join_raw(COUNTRY,DATA_TYPE,border = ""):
    # load two CSV files as dataframes and concat them. after, save a new CSV file 
    # whose name contains the start and end date of the dataframe
    all_df = None
    if (border == ""):
        files = glob.glob(f"./**/{COUNTRY}_{DATA_TYPE}_2*.csv",recursive=True)
    else:
        files = glob.glob(f"./**/{COUNTRY}_{DATA_TYPE}_{border}_2*.csv",recursive=True)

    for file_path in files:
        # read a csv file as dataframe. first row is a header and data starts at second row
        df = pd.read_csv(file_path, header = list(range(get_header_count(file_path))), index_col = 0) #fazer a lista dos headers apenas uma vez
        if all_df is not None:
            all_df = pd.concat([all_df, df])
        else:
            all_df = df

    # extract the start and end date of the dataframe index
    start_date = all_df.index[1]
    end_date = all_df.index[-1]
    print(start_date, end_date)

    # Convert the dates to string
    start_date_str = str(start_date).replace("+","-").replace(":","-")
    end_date_str = str(end_date).replace("+","-").replace(":","-")

    # save the dataframe as a CSV file
    if (border == ""):
        all_df.to_csv(f"data/{COUNTRY}/{DATA_TYPE}/{COUNTRY}_all_{DATA_TYPE}_{start_date_str}-{end_date_str}_raw.csv")
    else:
        all_df.to_csv(f"data/{COUNTRY}/{DATA_TYPE}/{COUNTRY}_to_{border}/{COUNTRY}_to_{border}_all_{DATA_TYPE}_{start_date_str}-{end_date_str}_raw.csv")
    all_df.index = pd.to_datetime(all_df.index)
    return all_df