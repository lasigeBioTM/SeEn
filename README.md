# SeEn
Sequential Enrichment of Recommendation Datasets

All the data used on this work is available at: Barros, Marcia (2022): seen_datasets. figshare. Dataset. https://doi.org/10.6084/m9.figshare.18857549.v1

# Astro datasets

## Create the sequential dataset ARM
git: https://github.com/lasigeBioTM/cARM

input files: 
Dias_updated_catalogue: /seen_datasets/astro_data/CATALOG-FINAL.csv

1. Follow the Configuration steps on README

 output is a sqlite database. 
The one created and used in this work is available at: /seen_datasets/astro_data/arm2021.db

2. Create the sequential dataset:

``
cd authorClusterMatrix/
python createAuthorClusterMatrix_seq.py
``
* aRMSeq file available at:  /seen_datasets/astro_data/astro20_user_item_rating_item_name_year_20_ordered.csv

## SeEn: https://github.com/lasigeBioTM/SeEn

# create the enriched dataset ARM
1. Create the similarity dataset for the open clusters of stars:

``
python ARM/astro_clusters_similarity/create_features_file.py
``

Used in this work: 
- input: membership tables: /seen_datasets/astro_data/memberships-tables/
- output: features file: /seen_datasets/astro_data/clusters_features_mean.csv

``
ARM/astro_clusters_similarity/create_sim_file.py
``

Used in this work: 
- input: features file: /seen_datasets/astro_data/clusters_features_mean.csv
- output: similarity file: /seen_datasets/astro_data/clusters_similatiry.csv

2. create the enriched datasets: 
``
python ARM/astro_sampler/astro_sampler.py
``

Used in this work: 
- input:similarity :  /seen_datasets/astro_data/clusters_similatiry.csv
- output: enriched datasets: /seen_datasets/astro_data/seen_cos/

The datasets are ready to be tested. 

#################################

## Chemical datasets


# Create the sequential dataset chERM
https://github.com/lasigeBioTM/CheRM 

* used in this work (original sequence): /seen_datasets/chem_data/cherm_20_order_year.csv

# Create the database with the semantic similarity
https://github.com/lasigeBioTM/SemanticSimDBcreator 

- Used in this work: /seen_datasets/chem_data/cherm_sim_20_dump.sql

# create the enriched dataset

``
python CHERM/semantic_sampler/src/main.py
``

- Used in this work (enriched sequences):  /seen_datasets/chem_data/sim_lin/

