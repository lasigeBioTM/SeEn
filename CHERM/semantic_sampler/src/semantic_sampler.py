import numpy as np
import pandas as pd
import sys
from data import *
np.set_printoptions(suppress=True)
import csv
import gc



def get_n_less_similar(df, item, n):

    """
    receives the most similar items and return them in a pd dataframe
    :param df: df with most similar items
    :param item: item
    :return: pd Dataframe with [comp, sim_lin]
    """

    df_comp_1 = df[df.comp_1 != item][['comp_1', 'sim_lin']].rename(columns={"comp_1": "comp"})
    df_comp_2 = df[df.comp_2 != item][['comp_2', 'sim_lin']].rename(columns={"comp_2": "comp"})
    most_sim_items = df_comp_1.append(df_comp_2).sort_values(by='sim_lin', ascending=True)
    most_sim_items = most_sim_items.head(n)

    return most_sim_items


def get_n_most_similar(df, item, n):

    """
    receives the most similar items and return them in a pd dataframe
    :param df: df with most similar items
    :param item: item
    :return: pd Dataframe with [comp, sim_lin]
    """

    df_comp_1 = df[df.comp_1 != item][['comp_1', 'sim_lin']].rename(columns={"comp_1": "comp"})
    df_comp_2 = df[df.comp_2 != item][['comp_2', 'sim_lin']].rename(columns={"comp_2": "comp"})
    most_sim_items = df_comp_1.append(df_comp_2).sort_values(by='sim_lin', ascending=False)
    most_sim_items = most_sim_items.head(n)

    return most_sim_items


def count_items(count, total):

    if (count % 1) == 0:
        print('Completed ', count, ' of ', total)


def remove_n_items(lst, n):
    lst = lst[:len(lst) - n]

    return lst


def sample_dataset_semantic_fast_add_after_positive_negative(mydb, dataset, n, csv_out, host, user_db, password, database):
    """
    write the original, then the n most similars and the n less similar items
    :param mydb: database with semantic similarity
    :param dataset: pandas dataframe with user, item, rating
    :return: pandas dataframe with user, item, rating, flag
    """
    mydb = connect(host, user_db, password, database)
    file = open(csv_out, 'a+', newline='')

    writer = csv.writer(file, delimiter=',')

    count = 0
    new_dataset = []

    users_unique = np.unique(dataset.user)
    print(users_unique.shape)


    for u in np.arange(len(users_unique)):
        print(u)

        user = users_unique[u]
        user_items = np.unique(dataset[dataset.user == user].item)

        print('get item from db')
        item_item_sim = get_all_similar_items_by_user(mydb, user_items)
        print('got item from db', item_item_sim.shape)

        count_ite = 0
        for item in dataset[dataset.user == user].item:

            if count_ite == len(dataset[dataset.user == user].item)-1:
                original = [[user, item, 1.0, 1]]

                writer.writerows(original)
                count_ite = 0
                continue

            item_in_comp = item_item_sim[(item_item_sim.comp_1 == item) | (item_item_sim.comp_2 == item)].sort_values('sim_lin',  ascending=False)

            similar_items = get_n_most_similar(item_in_comp, item, n)
            less_similar_items = get_n_less_similar(item_in_comp, item, n)

            print()
            similar_items = pd.concat([similar_items, less_similar_items])

            similar_items['user'] = int(user)
            similar_items['flag'] = int(0)

            similar_items = similar_items[['user', 'comp', 'sim_lin', 'flag']]
            similar_items['comp'] = similar_items.comp.astype('int64')

            similar_items = similar_items.values.tolist()
            #similar_items.to_csv(csv_out, mode='a', index=False, header=False)

            original = [[user, item, 1, 1]]

            writer.writerows(original)

            for i in similar_items:
                writer.writerow(i)


                #new_dataset.append(i)

            #original = pd.DataFrame(np.array([user, item, 1, 1]))
            #original.T.to_csv(csv_out, mode='a', index=False, header=False)

            # #new_dataset.append(original)

            del similar_items
            del item_in_comp
            del original
            gc.collect()
            file.flush()

            count_ite+=1

        count_items(count, len(users_unique))
        sys.stdout.flush()
        count += 1
        del item_item_sim
        del user_items
        gc.collect()
    file.close()


def write_to_csv(dataset, user, writer, item_item_sim, n):

    count_ite = 0

    for item in dataset[dataset.user == user].item:

        if count_ite == len(dataset[dataset.user == user].item) - 1:
            original = [[user, item, 1.0, 1]]

            writer.writerows(original)
            count_ite = 0
            continue

        item_in_comp = item_item_sim[(item_item_sim.comp_1 == item) | (item_item_sim.comp_2 == item)].sort_values(
            'sim_lin', ascending=False)

        similar_items = get_n_most_similar(item_in_comp, item, n)

        similar_items['user'] = int(user)
        similar_items['flag'] = int(0)

        similar_items = similar_items[['user', 'comp', 'sim_lin', 'flag']]
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



def sample_dataset_semantic_fast_add_after_multiple_files(dataset, n, path_to_directory, host, user_db, password, database):
    """
    write the original, then the n most similars, multiple csv files with diferent n
    :param mydb: database with semantic similarity
    :param dataset: pandas dataframe with user, item, rating
    :return: pandas dataframe with user, item, rating, flag
    """

    mydb = connect(host, user_db, password, database)

    # file = open(csv_out, 'a+', newline='')
    #
    # writer = csv.writer(file, delimiter=',')
    count = 0
    users_unique = np.unique(dataset.user)
    print(users_unique.shape)

    for u in np.arange(len(users_unique)):
        print(u)

        user = users_unique[u]
        user_items = np.unique(dataset[dataset.user == user].item)

        print('get item from db')
        item_item_sim = get_all_similar_items_by_user(mydb, user_items)
        print('got item from db', item_item_sim.shape)


        for i in range(1, n+1):

            file = open(path_to_directory + "cherm_20_similarity_sim_lin_" + str(i) + "_sampled_after.csv", 'a+', newline='')
            writer = csv.writer(file, delimiter=',')

            write_to_csv(dataset, user, writer, item_item_sim, i)

            file.close()

        count_items(count, len(users_unique))
        sys.stdout.flush()
        count += 1
        del item_item_sim
        del user_items
        gc.collect()


def sample_dataset_semantic_fast_add_after(mydb, dataset, n, csv_out, host, user_db, password, database):
    """
    write the original, then the n most similars
    :param mydb: database with semantic similarity
    :param dataset: pandas dataframe with user, item, rating
    :return: pandas dataframe with user, item, rating, flag
    """
    mydb = connect(host, user_db, password, database)
    file = open(csv_out, 'a+', newline='')

    writer = csv.writer(file, delimiter=',')

    count = 0
    new_dataset = []

    users_unique = np.unique(dataset.user)
    print(users_unique.shape)


    for u in np.arange(len(users_unique)):
        print(u)

        user = users_unique[u]
        user_items = np.unique(dataset[dataset.user == user].item)

        print('get item from db')
        item_item_sim = get_all_similar_items_by_user(mydb, user_items)
        print('got item from db', item_item_sim.shape)

        count_ite = 0
        for item in dataset[dataset.user == user].item:

            if count_ite == len(dataset[dataset.user == user].item)-1:
                original = [[user, item, 1.0, 1]]

                writer.writerows(original)
                count_ite = 0
                continue

            item_in_comp = item_item_sim[(item_item_sim.comp_1 == item) | (item_item_sim.comp_2 == item)].sort_values('sim_lin',  ascending=False)

            similar_items = get_n_most_similar(item_in_comp, item, n)

            similar_items['user'] = int(user)
            similar_items['flag'] = int(0)

            similar_items = similar_items[['user', 'comp', 'sim_lin', 'flag']]
            similar_items['comp'] = similar_items.comp.astype('int64')



            similar_items = similar_items.values.tolist()
            #similar_items.to_csv(csv_out, mode='a', index=False, header=False)


            original = [[user, item, 1, 1]]

            writer.writerows(original)

            for i in similar_items:
                writer.writerow(i)


                #new_dataset.append(i)

            #original = pd.DataFrame(np.array([user, item, 1, 1]))
            #original.T.to_csv(csv_out, mode='a', index=False, header=False)

            # #new_dataset.append(original)

            del similar_items
            del item_in_comp
            del original
            gc.collect()
            file.flush()

            count_ite+=1

        count_items(count, len(users_unique))
        sys.stdout.flush()
        count += 1
        del item_item_sim
        del user_items
        gc.collect()
    file.close()

def sample_dataset_semantic_fast(mydb, dataset, n, csv_out, host, user_db, password, database):
    """

    :param mydb: database with semantic similarity
    :param dataset: pandas dataframe with user, item, rating
    :return: pandas dataframe with user, item, rating, flag
    """
    mydb = connect(host, user_db, password, database)


    count = 0
    new_dataset = []

    users_unique = np.unique(dataset.user)
    print(users_unique.shape)


    for user in users_unique:
        file = open(csv_out, 'a+', newline='')

        writer = csv.writer(file, delimiter=',')

        user_items = np.unique(dataset[dataset.user == user].item)

        print('get item from db')
        item_item_sim = get_all_similar_items_by_user(mydb, user_items)
        print('got item from db', item_item_sim.shape)

        for item in dataset[dataset.user == user].item:
            item_in_comp = item_item_sim[(item_item_sim.comp_1 == item) | (item_item_sim.comp_2 == item)].sort_values('sim_lin',  ascending=False)

            similar_items = get_n_most_similar(item_in_comp, item, n)

            similar_items['user'] = int(user)
            similar_items['flag'] = int(0)

            similar_items = similar_items[['user', 'comp', 'sim_lin', 'flag']]
            similar_items['comp'] = similar_items.comp.astype('int64')



            similar_items = similar_items.values.tolist()
            #similar_items.to_csv(csv_out, mode='a', index=False, header=False)

            for i in similar_items:
                writer.writerow(i)


                #new_dataset.append(i)

            #original = pd.DataFrame(np.array([user, item, 1, 1]))
            #original.T.to_csv(csv_out, mode='a', index=False, header=False)
            original = [[user, item, 1, 1]]

            writer.writerows(original)
            # #new_dataset.append(original)

            del similar_items
            del item_in_comp
            del original
            gc.collect()
            file.flush()

        count_items(count, len(users_unique))
        sys.stdout.flush()
        count += 1
        del item_item_sim
        del user_items
        gc.collect()
        file.close()


        item_item_sim = ''




    # df_new_dataset = pd.DataFrame(np.array(new_dataset), columns=['user', 'item', 'rating', 'flag'])
    # df_new_dataset.user = df_new_dataset.user.astype('int64')
    # df_new_dataset.item = df_new_dataset.item.astype('int64')
    # df_new_dataset.flag = df_new_dataset.flag.astype('int64')

    #return df_new_dataset



def sample_dataset_semantic(mydb, dataset):
    """

    :param mydb: database with semantic similarity
    :param dataset: pandas dataframe with user, item, rating
    :return: pandas dataframe with user, item, rating, flag
    """

    count = 0
    new_dataset = []

    dataset = np.array(dataset)
    for i in range(len(dataset)):

        item = dataset[i][1]

        similar_items = get_similar_items(mydb, item)
        similar_items = get_n_most_similar(similar_items, item)

        similar_items = np.array(similar_items)


        for it in similar_items:
            #intert user id
            it = np.insert(it, 0, dataset[i][0])
            # insert flag
            it = np.insert(it, 3, 1)

            # append to list
            new_dataset.append(it.tolist())

        # mark orifinal with flah 0 and append to list
        line = dataset[i]
        line = np.insert(line, 3, 0)
        new_dataset.append(line.tolist())

        count_items(count, len(dataset))
        sys.stdout.flush()
        count+=1


    df_new_dataset = pd.DataFrame(np.array(new_dataset), columns=['user', 'item', 'rating', 'flag'])
    df_new_dataset.user = df_new_dataset.user.astype('int64')
    df_new_dataset.item = df_new_dataset.item.astype('int64')
    df_new_dataset.flag = df_new_dataset.flag.astype('int64')

    return df_new_dataset


def sample_dataset_random(dataset, n_items, csv_out):
    """

    :param mydb: database with semantic similarity
    :param dataset: pandas dataframe with user, item, rating
    :return: pandas dataframe with user, item, rating, flag
    """
    # count = 0
    # new_dataset = []
    #
    # dataset_array = np.array(dataset)
    # for i in range(len(dataset_array)):
    #
    #     random_items = np.random.choice(dataset.item, n_items)
    #     random_ratings = np.zeros(n_items)
    #     random_items = np.column_stack((random_items, random_ratings))
    #
    #     # insert original
    #     line = dataset_array[i]
    #     line = np.insert(line, 3, 0)
    #     new_dataset.append(line.tolist())
    #
    #     # insert random items in the sequence
    #     count_ite = 0
    #     for it in random_items:
    #
    #         if count_ite == len(line)-1:
    #
    #             count_ite = 0
    #             continue
    #
    #         # insert user in position 0
    #         it = np.insert(it, 0, dataset_array[i][0])
    #         # insert flag new item in position 3
    #         it = np.insert(it, 3, 1)
    #
    #         new_dataset.append(it.tolist())
    #         count_ite+=1
    #
    #
    #     count_items(count, len(dataset_array))
    #     sys.stdout.flush()
    #
    #     count+=1
    #
    # df_new_dataset = pd.DataFrame(np.array(new_dataset), columns=['user', 'item', 'rating', 'flag'])
    # df_new_dataset.user = df_new_dataset.user.astype('int64')
    # df_new_dataset.item = df_new_dataset.item.astype('int64')
    # df_new_dataset.flag = df_new_dataset.flag.astype('int64')



    file = open(csv_out, 'a+', newline='')

    writer = csv.writer(file, delimiter=',')

    users_unique = np.unique(dataset.user)
    print(users_unique.shape)


    for u in np.arange(len(users_unique)):
        print(u)

        user = users_unique[u]
        #user_items = np.unique(dataset[dataset.user == user].item)


        count_ite = 0
        for item in dataset[dataset.user == user].item:
            random_items = np.random.choice(dataset.item, n_items)
            random_ratings = np.zeros(n_items)
            random_items = np.column_stack((random_items, random_ratings))

            if count_ite == len(dataset[dataset.user == user].item)-1:
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

            count_ite +=1
