import pandas as pd
import numpy as np

def calculares_clusters_gaia_mean(df):

    df_mean = df[df.columns].mean(axis=0).to_frame().T

    return df_mean