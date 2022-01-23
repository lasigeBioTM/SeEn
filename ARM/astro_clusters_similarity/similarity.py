import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from scipy import sparse
from sklearn import preprocessing

def normalize_data(df):

    x = df.values  # returns a numpy array
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    df = pd.DataFrame(x_scaled)

    return df

def calculate_cosine_sim(df):
    df = df.dropna().reset_index(drop=True)

    new_df = df[['DE_ICRS', 'e_DE_ICRS', 'Plx']]
    new_df = normalize_data(new_df)
    print(new_df)
    A_sparse = sparse.csr_matrix(new_df)

    similarities = cosine_similarity(A_sparse)

    #similarities_sparse = cosine_similarity(A_sparse, dense_output=False)

    i, j = np.triu_indices_from(similarities, k=1)
    v = similarities[i, j]
    ijv = np.concatenate((i, j, v)).reshape(3, -1).T
    ijv = ijv[v != 0.0]
    sim_df = pd.DataFrame(ijv, columns=['item_1', 'item_2', 'cos_sim'])

    sim_df['item_1'] = sim_df['item_1'].astype('int64')
    sim_df['item_2'] = sim_df['item_2'].astype('int64')

    df = df.rename_axis('old_index').reset_index()


    sim_df["item1_name"] = sim_df["item_1"].map(df.set_index('old_index')["cluster"]).fillna(0)
    sim_df["item2_name"] = sim_df["item_2"].map(df.set_index('old_index')["cluster"]).fillna(0)

    sim_df[['item1_name', 'item2_name', 'cos_sim']].to_csv('/data/astro_data/clusters_similatiry.csv', mode='w', header=True, index=False)






