import numpy as np
import pandas as pd
import configargparse
import sys
from data import *
from semantic_sampler import *


if __name__ == '__main__':

    # ratings = pd.read_csv('mlData/u.data', sep='\t',
    #                     names=['user', 'item', 'rating', 'timestamp'])

    p = configargparse.ArgParser(default_config_files=['../config/config.ini'])
    p.add('-mc', '--my-config', is_config_file=True, help='alternative config file path')

    p.add("-ds", "--path_to_dataset", required=False, help="path to dataset", type=str)
    p.add("-ds_samp", "--path_to_updated_csv", required=False, help="path to updated csv", type=str)
    p.add("-dir", "--path_to_directory", required=False, help="path to directory", type=str)
    p.add("-n", "--n", required=False, help="n most similar items", type=int)

    p.add("-host", "--host", required=False, help="db host", type=str)
    p.add("-user", "--user", required=False, help="db user", type=str)
    p.add("-pwd", "--password", required=False, help="db password", type=str)
    p.add("-db_name", "--database", required=False, help="db name", type=str)

    options = p.parse_args()

    path_to_dataset = options.path_to_dataset
    path_to_updated_csv = options.path_to_updated_csv
    path_to_directory = options.path_to_directory
    n = options.n
    host = options.host
    user = options.user
    password = options.password
    database = options.database

    #################################################################################################################

    df_dataset = read_dataset(path_to_dataset, 2)
    df_dataset = df_dataset[['user', 'item', 'rating']]
    print(df_dataset)


    ##################################################################################################################
    # connect db
    mydb = connect(host, user, password, database)

    ##################################################################################################################
    # create new dataset with extra items
    # new_dataset = sample_dataset_semantic(mydb, df_dataset)
    # use: sample_dataset_random; sample_dataset_semantic_fast_add_after_multiple_files

    #new_dataset = sample_dataset_random(df_dataset, 2)
    #new_dataset = sample_dataset_random(df_dataset, 5, path_to_updated_csv)
    #new_dataset = sample_dataset_semantic_fast(mydb, df_dataset, n, path_to_updated_csv, host, user, password, database)
    #new_dataset = sample_dataset_semantic_fast_add_after(mydb, df_dataset, n, path_to_updated_csv, host, user, password, database)
    new_dataset = sample_dataset_semantic_fast_add_after_multiple_files(df_dataset, n, path_to_directory, host, user, password, database)
    #new_dataset = sample_dataset_semantic_fast_add_after_positive_negative(mydb, df_dataset, n, path_to_updated_csv, host, user, password, database)


