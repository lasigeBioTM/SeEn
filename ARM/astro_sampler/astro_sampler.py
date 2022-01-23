import numpy as np
import pandas as pd
import configargparse
import sys
from data import *
import numpy as np
import pandas as pd
import sys
from data import *

np.set_printoptions(suppress=True)
import csv
import gc


def count_items(count, total):

    if (count % 1) == 0:
        print('Completed ', count, ' of ', total)


def get_n_most_similar(df, item, n):

    """
    receives the most similar items and return them in a pd dataframe
    :param df: df with most similar items
    :param item: item
    :return: pd Dataframe with [comp, sim_lin]
    """

    df_comp_1 = df[df.id_item_1 != item][['id_item_1', 'sim_cos']].rename(columns={"id_item_1": "comp"})
    df_comp_2 = df[df.id_item_2 != item][['id_item_2', 'sim_cos']].rename(columns={"id_item_2": "comp"})
    most_sim_items = df_comp_1.append(df_comp_2).sort_values(by='sim_cos', ascending=False)
    most_sim_items = most_sim_items.head(n)

    return most_sim_items

def write_to_csv(dataset, user, writer, item_item_sim, n):

    count_ite = 0

    for item in dataset[dataset.user == user].item:

        if count_ite == len(dataset[dataset.user == user].item) - 1:
            original = [[user, item, 1.0, 1]]

            writer.writerows(original)
            count_ite = 0
            continue

        item_in_comp = item_item_sim[(item_item_sim.id_item_1 == item) | (item_item_sim.id_item_2 == item)].sort_values(
            'sim_cos', ascending=False)

        similar_items = get_n_most_similar(item_in_comp, item, n)

        similar_items['user'] = int(user)
        similar_items['flag'] = int(0)

        similar_items = similar_items[['user', 'comp', 'sim_cos', 'flag']]
        similar_items['comp'] = similar_items.comp.astype('int64')

        similar_items = similar_items.values.tolist()
        # similar_items.to_csv(csv_out, mode='a', index=False, header=False)

        original = [[user, item, 1, 1]]

        writer.writerows(original)

        for i in similar_items:
            writer.writerow(i)

            # new_dataset.append(i)

        # original = pd.DataFrame(np.array([user, item, 1, 1]))
        # original.T.to_csv(csv_out, mode='a', index=False, header=False)

        # #new_dataset.append(original)

        del similar_items
        del item_in_comp
        del original
        gc.collect()

        count_ite += 1


def sample_dataset_semantic_fast_add_after_multiple_files(dataset, n, path_to_directory, sim_df):
    """
    write the original, then the n most similars, multiple csv files with diferent n
    :param mydb: database with semantic similarity
    :param dataset: pandas dataframe with user, item, rating
    :return: pandas dataframe with user, item, rating, flag
    """

    #mydb = connect(host, user_db, password, database)

    # file = open(csv_out, 'a+', newline='')
    #
    # writer = csv.writer(file, delimiter=',')
    count = 0
    users_unique = np.unique(dataset.user)
    print(users_unique.shape)

    for u in np.arange(len(users_unique)):
        print(u, '-', len(users_unique))

        user = users_unique[u]
        user_items = np.unique(dataset[dataset.user == user].item)

        print('get item from db')
        item_item_sim = get_all_similar_items_by_user(sim_df, user_items)
        print('got item from db', item_item_sim.shape)

        for i in range(1, n + 1):
            file = open(path_to_directory + "astro20_user_item_rating_ordered_sim_cos" + str(i) + "_sampled_after.csv", 'a+',
                        newline='')
            writer = csv.writer(file, delimiter=',')

            write_to_csv(dataset, user, writer, item_item_sim, i)

            file.close()

        count_items(count, len(users_unique))
        sys.stdout.flush()
        count += 1
        del item_item_sim
        del user_items
        gc.collect()


def sample_dataset_random(dataset, n_items, csv_out):
    """

    :param mydb: database with semantic similarity
    :param dataset: pandas dataframe with user, item, rating
    :return: pandas dataframe with user, item, rating, flag
    """

    file = open(csv_out, 'a+', newline='')

    writer = csv.writer(file, delimiter=',')

    users_unique = np.unique(dataset.user)
    print(users_unique.shape)

    for u in np.arange(len(users_unique)):
        print(u, '-', len(users_unique))

        user = users_unique[u]
        # user_items = np.unique(dataset[dataset.user == user].item)

        count_ite = 0
        for item in dataset[dataset.user == user].item:

            random_items = np.random.choice(dataset.item, n_items)

            random_ratings = np.zeros(n_items)

            random_items = np.column_stack((random_items, random_ratings))

            if count_ite == len(dataset[dataset.user == user].item) - 1:
                original = [[user, item, 1.0, 1]]

                writer.writerows(original)
                count_ite = 0

                continue

            original = [[user, item, 1, 1]]

            writer.writerows(original)

            for i in random_items:
                i = np.insert(i, 0, user)
                i = np.insert(i, 3, 0)

                writer.writerow(i)

            random_items = []

            count_ite += 1


if __name__ == '__main__':
    path_to_dataset = '/data/astro_data/user_item_rating_user_name_item_name_year.csv'
    path_to_updated_csv = '/data/astro_data/astro20_user_item_rating_ordered_rand1_2.csv'
    # path_to_updated_csv = '/data/astro_data/astro20_user_item_rating_ordered_sim_cos10.csv'
    path_to_sim_file = '/data/astro_data/clusters_similatiry.csv'
    db_file = '/cARM2021/arm2021.db'
    path_to_directory = '/data/astro_data/sim_cos/'


    #################################################################################################################

    df_dataset = read_dataset(path_to_dataset, 2)
    df_dataset = df_dataset[['user', 'item', 'rating', 'item_name']]

    df_sim = pd.read_csv(path_to_sim_file, skiprows=1, names=['item_1', 'item_2', 'sim_cos'], sep=',')

    ##################################################################################################################
    # open similarities file
    # mydb = connect(host, user, password, database)
    conn = create_connection(db_file)
    clusters_df = get_clusters_table(conn)


    ## mapping original name to id in item_1 and item_2

    #df["item_name"] = df["idCluster"].map(clusters.set_index('id')["name"]).fillna(0)
    df_sim["id_item_1"] = df_sim["item_1"].map(clusters_df.set_index('original_name')["id"])
    df_sim["id_item_2"] = df_sim["item_2"].map(clusters_df.set_index('original_name')["id"])
    df_sim = df_sim.dropna() # because there are clusters not in rs dataset
    df_sim["id_item_1"] = df_sim["id_item_1"].astype('int64')
    df_sim["id_item_2"] = df_sim["id_item_2"].astype('int64')


    ##################################################################################################################
    # create new dataset with extra items

    sample_dataset_random(df_dataset, 1, path_to_updated_csv)
    #sample_dataset_semantic_fast_add_after_multiple_files(df_dataset, 10, path_to_directory, df_sim)
