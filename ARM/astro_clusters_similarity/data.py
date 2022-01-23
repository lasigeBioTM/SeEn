import pandas as pd
import numpy as np


def open_dat_file(dat_path):
    df = pd.read_table(dat_path, sep='\s\s+', engine='python')

    return df

def open_dat_file(csv_file):

    df = pd.read_csv(csv_file)

    return df
