import openmatrix as omx
import numpy as np
import pandas as pd
import os
import time
import multiprocessing as mp

# some data sources come from a previous 1-zone run so that 
# fields like income segments are available. 
data_store = pd.HDFStore('E:\my_test_example_mp\output\pipeline.h5', 'r')
data_path = r'E:\my_test_example_multizone\data'
output_directory = r'E:\my_test_example_multizone\outputs'

# currently, code chunks table created in step 1 
# (30 TAZs sampled for every chooser) by number of 
# processors used. Must have enough RAM to hold this table .
procs = 30
chunk_size = procs

# data inputs
omx_file = omx.open_file(os.path.join(data_path, 'skims.omx'))
taz_df = pd.read_csv(os.path.join(data_path, 'taz.csv'))
maz_df = pd.read_csv(os.path.join(data_path, 'maz.csv'))
land_use_df = pd.read_csv(os.path.join(data_path, 'land_use.csv'))

persons = data_store['/persons/initialize_households']
households = data_store['/households/initialize_households']
    

def init_pool(merge_df):
    """
   Create a global variable that can be shared across processes.
   Use for the taz_maz table. 
    """
    global global_merge_df
    global_merge_df = merge_df

def sample(df):
    """
    Select 1 MAZ for each TAZ selected in initial TAZ-based sampling.
    
    For each chooser, an ID must be used to represent each unique sample, 
    because it is possible to select the same TAZ more than once per chooser in
    the initial TAZ-based sampling step. This does not include probability weights, 
    which could be based on size terms and/or distance. 
    
    This function is used for multiprocessing. 
    """
    df = df.merge(global_merge_df, left_on='dtaz', right_on='TAZ', how='inner')
    samples = df.groupby(['chooser_id', 'sample_id']).sample(n=1, random_state=1)
    return samples



if __name__ == '__main__':
    start = time.time()
    
    zones_with_employment = land_use_df[land_use_df['TOTEMP'] > 0]['zone_id'].unique()

    households = households[['income_segment']]
    persons = persons.merge(households, left_on ='household_id', right_index = True)

    workers = persons[persons['is_worker'] == 1]
    workers.reset_index(inplace = True)
    
    zones_with_workers = workers['home_zone_id'].unique()
    
    dist_skim = omx_file['SOVTOLL_DIST__AM']

    choosers_list = []
    samples_list = []

##### STEP 1- meant to replicate the output of current sampling using TAZs only #####

# this step is only meant to build the data for step 2, not presenting design ideas here. 

# loop through every TAZ that has workers, generate 30 samples per chooser. 
    for taz in zones_with_workers:
        choosers = workers[workers['home_zone_id']==taz]['person_id']
        samples = np.stack([np.random.choice(zones_with_employment, 30, replace=True) for i in range(len(choosers))])
        choosers_list.append(choosers)
        samples_list.append(samples)

    # concat our list of arrays into one array for choosers and samples
    choosers_final = np.concatenate(choosers_list)

    #create a df using chooser ids and samples 
    samples_final = np.concatenate(samples_list)
    taz_based_samples = pd.DataFrame(samples_final, index = choosers_final)
    taz_based_samples = taz_based_samples.stack().reset_index()
    taz_based_samples = taz_based_samples.rename(columns = {'level_0' : 'chooser_id', 0: 'dtaz'})
    taz_based_samples = taz_based_samples[['chooser_id', 'dtaz']]
    taz_based_samples['sample_id'] = taz_based_samples.groupby('chooser_id').cumcount()

##### STEP 2- Sample MAZs from parent TAZs #####
    
##### multiprocessing
    df_chunks = np.array_split(test, chunk_size)
    pool = mp.Pool(procs, init_pool, [maz_df])
    results = pool.map(sample, df_chunks)
    pool.close()
    # concat the results from each process
    results = pd.concat(results)
    results.to_csv(os.path.join(output_directory, r'E:\test.csv'))
    end = time.time()
    print(end - start)
    
