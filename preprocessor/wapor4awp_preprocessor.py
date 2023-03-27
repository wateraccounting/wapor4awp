import os

import sys
from time import sleep
import gc
from pathlib import Path
import rasterio
from rasterio.plot import show
from rasterio.enums import Resampling
import geopandas as gpd
import pandas as pd
import numpy as np
import rioxarray as riox
import xarray
import dask
from rasterio.features import shapes
import multiprocessing
from tqdm import tqdm

# Linux/OSX:
# multiprocessing.popen_spawn_posix
# Windows:
multiprocessing.popen_spawn_win32
import threading
from dask.distributed import Client, progress, LocalCluster, Lock
from dask.utils import SerializableLock
import warnings
import dask.array as da
import rasterio as rio
from shapely.geometry import mapping



warnings.simplefilter(action='ignore', category=FutureWarning)

# Generate Country Dataframe
country_names = gpd.read_file('Mask/wapor_region.zip')


    
def compute_aeti():
    # AETI conversion
    input_aeti_dir = "AETI_M"
    output_aeti_dir = "AETI_M_CORR"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_aeti_dir):
        os.makedirs(output_aeti_dir)

    for filename in tqdm(os.listdir(input_aeti_dir)):
        if filename.endswith(".tif"):
            input_aeti_file = os.path.join(input_aeti_dir, filename)
            output_file = os.path.join(output_aeti_dir, filename.replace(".tif", "_CORR.tif"))
            with riox.open_rasterio(input_aeti_file) as src:                 
                corrected = (src * 0.1).astype("int16") # Multiply with conversion factor or 0.1 and cast to int16
                corrected  = corrected.fillna(-9999)
                corrected = src.where((src >= 0) & (~src.isnull()), other=0)                
                corrected.rio.to_raster(output_file, compress="LZW")


def compute_pcp():
    # PCP conversion and resampling
    input_pcp_dir = "PCP_M"
    output_pcp_dir = "PCP_M_CORR"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_pcp_dir):
        os.makedirs(output_pcp_dir)

    # Resampling with AETI_resampler file
    resampler = riox.open_rasterio("AETI_resampler.tif")

    for filename in tqdm(os.listdir(input_pcp_dir)):
        if filename.endswith(".tif"):
            input_pcp_file = os.path.join(input_pcp_dir, filename)
            output_file = os.path.join(output_pcp_dir, filename.replace(".tif", "_CORR.tif"))
            with riox.open_rasterio(input_pcp_file) as src:
                resampled = src.rio.reproject_match(resampler, resampling=Resampling.bilinear)                
                resampled = (resampled * 0.1).astype("int16") # Multiply with conversion factor or 0.1 and cast to int16
                resampled  = resampled.fillna(-9999)
                resampled.rio.to_raster(output_file, compress="LZW")
                

def compute_pcp():
    # PCP conversion and resampling
    input_pcp_dir = "PCP_M"
    output_pcp_dir = "PCP_M_CORR"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_pcp_dir):
        os.makedirs(output_pcp_dir)

    # Resampling with AETI_resampler file
    resampler = riox.open_rasterio("AETI_resampler.tif")

    for filename in tqdm(os.listdir(input_pcp_dir)):
        if filename.endswith(".tif"):
            input_pcp_file = os.path.join(input_pcp_dir, filename)
            output_file = os.path.join(output_pcp_dir, filename.replace(".tif", "_CORR.tif"))
            with riox.open_rasterio(input_pcp_file) as src:
                resampled = src.rio.reproject_match(resampler, resampling=Resampling.bilinear)                
                resampled = (resampled * 0.1).astype("int16") # Multiply with conversion factor or 0.1 and cast to int16
                resampled  = resampled.fillna(-9999)
                corrected = src.where((src >= 0) & (~src.isnull()), other=0)
                resampled.rio.to_raster(output_file, compress="LZW")


def compute_pe():
    # PCP conversion and resampling
    input_pcp_dir = "PCP_M_CORR"
    output_pe_dir = "PE"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_pe_dir):
        os.makedirs(output_pe_dir)

    for filename in tqdm(os.listdir(input_pcp_dir)):
        if filename.endswith(".tif") and filename.startswith("L1_PCP_"):
            input_file = os.path.join(input_pcp_dir, filename)
            output_file = os.path.join(output_pe_dir, filename.replace("L1_PCP_", "L1_PE_"))
            
            # Check if output file already exists
            if os.path.exists(output_file):
                continue
            
            with riox.open_rasterio(input_file) as src:
                # Perform the PE computation
                pe = src.where(src > 75, (src * 0.8) - 25)
                pe = pe.where(pe >= 0, 0)
                pe = pe.where(src <= 75, np.fmax((src * 0.6) - 10, 0))
                pe = pe.fillna(-9999)
                
                # Save the result
                pe.rio.to_raster(output_file, compress="LZW")
                


def compute_etb_m():
    # Define input and output directories
    input_aeti_dir = "AETI_M_CORR"
    input_pe_dir = "PE"
    output_dir = "Etb_M"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Define chunk size for dask
    chunk_size = "auto"

    # Loop through AETI_M_CORR directory
    for aeti_file in tqdm(os.listdir(input_aeti_dir)):
        if aeti_file.startswith("L1_AETI_") and aeti_file.endswith(".tif"):
            
            # Find matching file in PE directory
            pe_file = "L1_PE_" + "_".join(aeti_file.split("_")[2:])            

            if pe_file in os.listdir(input_pe_dir):
                # Load AETI and PE files using dask
                aeti_path = os.path.join(input_aeti_dir, aeti_file)
                pe_path = os.path.join(input_pe_dir, pe_file)
                with rio.open(aeti_path) as aeti_src, rio.open(pe_path) as pe_src:
                    aeti = da.from_array(aeti_src.read(1), chunks=chunk_size).astype(pe_src.profile['dtype'])
                    pe = da.from_array(pe_src.read(1), chunks=chunk_size).astype(aeti_src.profile['dtype'])

                    # Compute Etb and create new file using numpy
                    etb = da.where(aeti - pe >= 0, pe - aeti, 0) # corrected from pe - aeti
                    etb = np.nan_to_num(etb)
                    output_file = "L1_Etb_" + "_".join(pe_file.split("_")[2:]) 
                    output_path = os.path.join(output_dir, output_file)
                    with rio.open(output_path, 'w', **aeti_src.profile) as dst:
                        dst.write(etb.astype(aeti_src.profile['dtype']), 1)



# Generate the individual CSV files
def get_raster_data(raster_data_dir, raster_start, data_type, landuse_data_dir):
    """
    Generate raster values per each landuse per country per month.
    """
    columns = ['country', 'landuse', 'year', 'filename', 'sum_per_month', 'data_type']
    landuses_df = pd.DataFrame(columns=columns)
    
    years = [x for x in range(2010, 2022)]
    ybar = tqdm(years)
    
    country_list = list(country_names.ROMNAM)
    cbar = tqdm(country_list)
    
    land_usage_list = [41, 42]
    
    for year in ybar:
        year_code = str(year)[2:]
        img_list = [f for f in Path(raster_data_dir).glob('**/*.tif') 
                    if f.stem.startswith(f'{raster_start}{year_code}')]
        lcc_ras = [f for f in Path(landuse_data_dir).glob('**/*.tif') if f.stem.endswith(f'{year_code}')][0]
        
        #print(lcc_ras)
        
        pbar = tqdm(img_list)
        
        for country in cbar:
            country_boundary = (country_names.iloc[np.where(country_names.ROMNAM == f'{country}')]).to_crs(4326)
            lcc_clip = get_raster_clip(lcc_ras, country_boundary)
            
            for rast in pbar:
                raster_clip = get_raster_clip(rast, country_boundary)
                
                for land_use in land_usage_list:
                    ybar.set_description(f"Processing Datasets for Year {year}")
                    cbar.set_description(f"Processing Datasets for {country}: {year}")
                    pbar.set_description(f"Processing Datasets {rast.stem} for {country}: {year}")
                    
                    print (land_use) 
                    
                    raster_lcc = extract_landuse_value(lcc_clip, raster_clip, land_use)
                    
                    #print (raster_lcc)
                    
                    image_df = get_raster_values(array_values=raster_lcc,
                                                 raster_name=rast,
                                                 country=country,
                                                 year=year,
                                                 land_usage=f'lcc_{land_use}',
                                                 data_type=data_type)
                    landuses_df = landuses_df.append(image_df)
                    
                    del image_df
                    del raster_lcc
                    gc.collect()

                del raster_clip
                gc.collect()
                
            del lcc_clip
            del country_boundary
            gc.collect()

    return landuses_df



# Aggregate the individual CSV files
def aggregate_csvs(aeti_file, pcp_file, pe_file, etb_file, output_file):
    aeti_df = pd.read_csv(aeti_file)
    pcp_df = pd.read_csv(pcp_file)
    pe_df = pd.read_csv(pe_file)
    etb_df = pd.read_csv(etb_file)

    aeti_df = aeti_df[['country', 'landuse', 'year', 'aeti']].rename(columns={'aeti': 'aeti'})
    pcp_df = pcp_df[['country', 'landuse', 'year', 'pcp']].rename(columns={'pcp': 'pcp'})
    pe_df = pe_df[['country', 'landuse', 'year', 'pe']].rename(columns={'pe': 'pe'})
    etb_df = etb_df[['country', 'landuse', 'year', 'Etb']].rename(columns={'Etb': 'etb'})

    aggregated_df = aeti_df.merge(pcp_df, on=['country', 'landuse', 'year'])
    aggregated_df = aggregated_df.merge(pe_df, on=['country', 'landuse', 'year'])
    aggregated_df = aggregated_df.merge(etb_df, on=['country', 'landuse', 'year'])

    aggregated_df.to_csv(output_file, index=False)







# Aggregate the values as yearly sums for each country
def aggregate_yearly(input_file, output_file):
    aggregated_df = pd.read_csv(input_file)
    aggregated_yearly_df = aggregated_df.groupby(['country', 'landuse', 'year']).agg(
        {'aeti': 'sum', 'pcp': 'sum', 'pe': 'sum', 'etb': 'sum'}).reset_index()

    aggregated_yearly_df.to_csv(output_file, index=False)


# Merge the data from 'aggregate_yearly.csv' with 'mycsv.csv'
def merge_data(aggregate_yearly_file, mycsv_file, output_file):
    # Load the aggregated yearly data
    aggregated_yearly_df = pd.read_csv(aggregate_yearly_file)

    # Filter the aggregated yearly data to keep only landuse 42
    aggregated_yearly_df = aggregated_yearly_df[aggregated_yearly_df['landuse'] == 'lcc_42']

    # Load the 'mycsv.csv' data
    mycsv_df = pd.read_csv(mycsv_file)

    # Merge the dataframes based on 'country'/'country_name' and 'year'
    merged_df = mycsv_df.merge(aggregated_yearly_df, left_on=['country_name', 'year'], right_on=['country', 'year'])

    # Select the desired columns from the merged dataframe
    selected_columns = [
        'country_name', 'id', 'fidd', 'STSCOD', 'lu_area_ratio', 'ISO-3', 'year',
        'area_41', 'area_42', 'area_ratio', 'Gva', '1-Cr', 'aeti', 'pcp', 'pe', 'etb'
    ]
    merged_df = merged_df[selected_columns]

    # Save the merged dataframe to a new CSV file
    merged_df.to_csv(output_file, index=False)





# Calculate the new attributes and save the results to 'awp_csv_n.csv'
def calculate_new_attributes(input_file, output_file):
    # Load the merged data
    merged_df = pd.read_csv(input_file)


    # Calculate new attributes aeti_n, pcp_m3, pe_n, and etb_n
    merged_df['aeti_m3'] = merged_df.apply(
        lambda row: (row['aeti'] / 1000) * 250 * 250 * row['area_42'] * 1000000, axis=1
    )
    merged_df['pcp_nm3'] = merged_df.apply(
        lambda row: (row['pcp'] / 1000) * 250 * 250 * row['area_42'] * 1000000, axis=1
    )
    merged_df['pe_m3'] = merged_df.apply(
        lambda row: (row['pe'] / 1000) * 250 * 250 * row['area_42'] * 1000000, axis=1
    )
    merged_df['etb_m3'] = merged_df.apply(
        lambda row: (row['etb'] / 1000) * 250 * 250 * row['area_42'] * 1000000, axis=1
    )


    # Calculate the new attributes
    merged_df['Va_Etb'] = merged_df.apply(
        lambda row: max(((row['etb'] / 1000) * 250 * 250 * row['area_42'] * 1000000), 0), axis=1
    )
    merged_df['Awp'] = merged_df.apply(
        lambda row: max((row['Gva'] * (row['1-Cr']) / row['Va_Etb']), 0) if row['Va_Etb'] > 0 else 0, axis=1
    )


    # Calculate tAwp
    merged_df['tAwp'] = 0
    for index, row in merged_df.iterrows():
        current_year = row['year']
        if current_year != 2010:
            previous_year = current_year - 1
            prev_row = merged_df[(merged_df['country_name'] == row['country_name']) & (merged_df['year'] == previous_year)]
            if not prev_row.empty:
                prev_awp = prev_row.iloc[0]['Awp']
                if prev_awp != 0:
                    merged_df.at[index, 'tAwp'] = ((row['Awp'] - prev_awp) / prev_awp) * 100
                else:
                    merged_df.at[index, 'tAwp'] = 0

    # Calculate cAwp
    base_year = 2015
    merged_df['cAwp'] = 0
    for index, row in merged_df.iterrows():
        base_row = merged_df[(merged_df['country_name'] == row['country_name']) & (merged_df['year'] == base_year)]
        if not base_row.empty:
            base_awp = base_row.iloc[0]['Awp']
            if base_awp != 0:
                merged_df.at[index, 'cAwp'] = ((row['Awp'] - base_awp) / base_awp) * 100
            else:
                merged_df.at[index, 'cAwp'] = 0

    # Save the updated dataframe to a new CSV file
    merged_df.to_csv(output_file, index=False)





def compute_lcc():
    # Define input and output directories
    input_dir = "LCC_A"
    output_dir = "LCC_41_42"

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Define chunk size for dask
    chunk_size = "auto"

    # Read the mask shapefile
    mask_file = "Mask/wapor_region.zip"
    mask = gpd.read_file(mask_file)

    # Loop through LCC_A directory
    lcc_files = sorted(os.listdir(input_dir))
    for lcc_file in tqdm(lcc_files):
        if lcc_file.endswith(".tif"):
            # Load LCC file using dask
            lcc_path = os.path.join(input_dir, lcc_file)
            lcc = da.from_array(rio.open(lcc_path).read(1), chunks=chunk_size)

            # Apply mask to the LCC file
            lcc_masked = lcc.rio.clip_mask(mask.geometry.apply(mapping), drop=False)

            # Filter landuse code 41 and save to file
            lcc_41 = da.where(lcc_masked == 41, lcc_masked, 0)
            lcc_41_sum = da.sum(lcc_41, axis=0)
            output_file_41 = os.path.join(output_dir, lcc_file.replace(".tif", "_41.tif"))
            with rio.open(lcc_path) as src:
                with rio.open(output_file_41, 'w', **src.profile) as dst:
                    dst.write(lcc_41_sum.astype(src.profile['dtype']), 1)

            # Filter landuse code 42 and save to file
            lcc_42 = da.where(lcc_masked == 42, lcc_masked, 0)
            lcc_42_sum = da.sum(lcc_42, axis=0)
            output_file_42 = os.path.join(output_dir, lcc_file.replace(".tif", "_42.tif"))
            with rio.open(lcc_path) as src:
                with rio.open(output_file_42, 'w', **src.profile) as dst:
                    dst.write(lcc_42_sum.astype(src.profile['dtype']), 1)    


if __name__ == '__main__':

    #compute_lcc()
    #compute_aeti()
    #filter_aeti()     
    #compute_pcp()    
    #compute_pe()
    compute_etb_m() 
    
    #compute_lcc()
    





    # # Generate the individual CSV files

    # # AETI Data Summation
    # aeti_df = get_raster_data(raster_data_dir='AETI_M_CORR', raster_start="L1_AETI_", data_type='AETI',
                              # landuse_data_dir='LCC_A')
    # aeti_df.to_csv('aeti_summation.csv')
    # del aeti_df
    # gc.collect()

    # # PCP Data Summation
    # pcp_df = get_raster_data(raster_data_dir='PCP_M_CORR', raster_start="L1_PCP_", data_type='PCP',
                             # landuse_data_dir='LCC_A')
    # pcp_df.to_csv('pcp_summation.csv')

    # del pcp_df
    # gc.collect()

    # # PE Data Summation
    # pe_df = get_raster_data(raster_data_dir='PE', raster_start="L1_PE_", data_type='PE',
                            # landuse_data_dir='LCC_A')
    # pe_df.to_csv('pe_summation.csv')
    # del pe_df
    # gc.collect()

    # Etb Data Summation
    etb_df = get_raster_data(raster_data_dir='Etb_M', raster_start="L1_Etb_", data_type='Etb',
                             landuse_data_dir='LCC_A')
    etb_df.to_csv('etb_summation.csv')
    del etb_df
    gc.collect()


    # Aggregate the individual CSV files
    aggregate_csvs('aeti_summation.csv', 'pcp_summation.csv', 'pe_summation.csv', 'etb_summation.csv', 'aggregated_csvs.csv')

    # Aggregate the values as yearly sums for each country
    aggregate_yearly('aggregated_csvs.csv', 'aggregated_yearly.csv')


    # Merge the data from 'aggregate_yearly.csv' with 'mycsv.csv'
    merge_data('aggregated_yearly.csv', 'mycsv.csv', 'merged_data.csv')


    # Calculate the new attributes and save the results to 'awp_csv_n.csv'
    calculate_new_attributes('merged_data.csv', 'awp_csv_n.csv')
