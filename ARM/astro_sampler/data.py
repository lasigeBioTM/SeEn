import mysql.connector
import numpy as np
import pandas as pd
import sys
import os.path
from os import path
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return None

def get_clusters_table(conn):

    df = pd.read_sql_query("SELECT * from clusters", conn)

    return df

# def connect(host, user, password, database):
#     engine = create_engine("mysql+pymysql://{user}:{pw}@172.17.0.3/{db}"
#                            .format(user=user,
#                                    pw=password,
#                                    db=database))
#
#     return engine


# def connect(host, user, password, database):
#     mydb = mysql.connector.connect(
#         host=host,
#         user=user,
#         password=password,
#         database=database
#     )
#
#     return mydb


def get_all_similar_items_by_user(sim_df, item_list):

    '''

    :param mydb:
    :param item:
    :return: item, sim_lin pd df
    '''

    items_1 = sim_df[sim_df.id_item_1.isin(item_list)]
    items_2 = sim_df[sim_df.id_item_2.isin(item_list)]

    results = pd.concat([items_1, items_2])

    return results

def get_similar_items(mydb, item):

    '''

    :param mydb:
    :param item:
    :return: item, sim_lin pd df
    '''

    sql = "select * from similarity where comp_1 = '%s' or comp_2 = '%s' order by sim_lin DESC limit 5 "
    results = pd.read_sql_query(sql % (item,item ), mydb)

    return results



def read_dataset(path_to_dataset, dataset_type):

    '''
    read csv
    :param path_to_dataset: path to csv
    :param dataset_type: 1: user, item, rating; 2: user, item, rating, year
    :return: pd Dataframe
    '''

    if dataset_type == 1:
        print('Reading data')
        data = pd.read_csv(path_to_dataset, names=['user', 'item', 'rating'], sep=',')

    elif dataset_type == 2:

        if path.exists('/data/astro_data/astro20_user_item_rating_item_name_year_20_ordered.csv'):
            data = pd.read_csv('/data/astro_data/astro20_user_item_rating_item_name_year_20_ordered.csv', names=['user', 'item', 'rating', 'item_name' ,'year'])
            print(data)

        else:
            print('Reading data')
            data = pd.read_csv(path_to_dataset, names=['user', 'item', 'rating',  'user_name', 'item_name', 'year'])

            print('Ordering by year')
            # year_splited = pd.DataFrame(data.year.str.split(' ', 2).tolist(),
            #                             columns=['year', 'month', 'day'])
            # data.year = year_splited.year

            data = data.groupby(['user'], sort=False) \
                .apply(lambda x: x.sort_values(['year'], ascending=True)) \
                .reset_index(drop=True)

            #data = data[['user', 'item']]
            #data = data.drop_duplicates()

            # select user with 20 or more items rated
            print('Filtering users by number of items rated')
            data = data.groupby('user').filter(lambda x: len(x) > 19)

            data[['user', 'item', 'rating', 'item_name', 'year']].to_csv('/data/astro_data/astro20_user_item_rating_item_name_year_20_ordered.csv', index=False, header=False)

    return data


def save_to_csv(data, path_to_csv):

    data.to_csv(path_to_csv, header=None, index=None, sep=',')