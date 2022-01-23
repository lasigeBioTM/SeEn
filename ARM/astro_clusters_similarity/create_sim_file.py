
from data import *
from similarity import *


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

    features_file = '/data/astro_data/clusters_features_mean.csv'

    df = open_dat_file(features_file)
    df_to_sim = df[['DE_ICRS', 'e_DE_ICRS', 'Plx', 'cluster']]
    print(df_to_sim)
    calculate_cosine_sim(df_to_sim)