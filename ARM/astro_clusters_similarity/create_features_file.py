import sys

import pandas as pd
import numpy as np
import configargparse
from data import *
from gaia_data import *
import os


if __name__ == '__main__':
    # p = configargparse.ArgParser(default_config_files=['../config.ini'])
    # p.add('-mc', '--my-config', is_config_file=True, help='alternative config file path')
    # p.add("-csvPath", "--clustersCSVpath", required=False, help="path to csv with clusters info", type=str)
    # p.add("-clustName", "--columnName", required=False, help="name of the column with the name of the clusters",
    #       type=str)
    # p.add("-limit", "--columnDevLimit", required=False, help="limit the number of clusters for dev",
    #       type=int)
    # p.add("-db", "--db_file", required=True, help="path do database",
    #       type=str)
    #
    # p.add("-tk", "--token", required=True, help="ADS API person token",
    #       type=str)
    #
    # p.add("-up", "--update", required=True, help="update database",
    #       type=bool)
    #
    # options = p.parse_args()
    #
    # aggloCSVPath = options.clustersCSVpath
    # columnName = options.columnName
    # columnLimit = options.columnDevLimit
    # db_file = options.db_file


    dat_directory = '/data/astro_data/memberships-tables/'

    dir_files = os.listdir(dat_directory)
    clusters_features = pd.DataFrame()

    count = 0

    for file in dir_files:
        print(file)
        print(count, ' - ', len(dir_files))

        df = open_dat_file(dat_directory + file)
        df_mean = calculares_clusters_gaia_mean(df)

        df_mean['cluster'] = file.split('.')[0]

        frames = [clusters_features, df_mean]
        clusters_features = pd.concat(frames)

        # if count == 10:
        #     print(clusters_features)
        #     sys.exit()

        count+=1


    print(clusters_features)

    clusters_features.to_csv('/data/astro_data/clusters_features_mean.csv', mode='w', header=True, index=False)
