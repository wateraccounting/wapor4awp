"""
August 2023
@author: Seleshi Yalew
"""
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

from rasterio.warp import calculate_default_transform, reproject, Resampling


warnings.simplefilter(action='ignore', category=FutureWarning)

# Generate Country Dataframe
country_names = gpd.read_file('Mask/wapor_region.zip')





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
                corrected = resampled.where((resampled >= 0) & (~resampled.isnull()), other=0)                
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
                resampled = resampled.where((resampled >= 0) & (~resampled.isnull()), other=0)
                resampled.rio.to_raster(output_file, compress="LZW")


                  
def compute_pcp_a():
    input_pcp_dir = "PCP_M_CORR"
    output_pcp_a_dir = "PCP_A_CORR"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_pcp_a_dir):
        os.makedirs(output_pcp_a_dir)

    # Get list of all PCP files
    pcp_files = [f for f in os.listdir(input_pcp_dir) if f.endswith('.tif') and f.startswith('L1_PCP_')]

    # Extract years from file names
    years = list(set([f[7:9] for f in pcp_files]))

    for year in tqdm(years):
        # Get all files for this year
        yearly_files = [f for f in pcp_files if f[7:9] == year]

        # Read and aggregate data, handling NoData values
        aggregated_data = None
        for file in yearly_files:
            file_path = os.path.join(input_pcp_dir, file)
            with rio.open(file_path) as src:
                data = src.read(1)

                # Assuming NoData values are represented as -9999, update to your specific NoData value
                nodata_value = -9999  
                data[data == nodata_value] = 0   # Replace NoData values with 0 or use a mask as per your requirements
                
                if aggregated_data is None:
                    aggregated_data = data
                else:
                    aggregated_data += data

        # Save aggregated data to new file
        output_file = os.path.join(output_pcp_a_dir, f'L1_PCP_A_{year}.tif')
        with rio.open(os.path.join(input_pcp_dir, yearly_files[0])) as src:
            meta = src.meta
            with rio.open(output_file, 'w', **meta) as dst:
                dst.write(aggregated_data, 1)


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
                
                pe =  pe.where(( pe >= 0) & (~ pe.isnull()), other=0)
                # Save the result
                pe.rio.to_raster(output_file, compress="LZW")
                

def compute_pcp_a():
    input_pcp_dir = "PCP_M_CORR"
    output_pcp_a_dir = "PCP_A_CORR"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_pcp_a_dir):
        os.makedirs(output_pcp_a_dir)

    # Get list of all PCP files
    pcp_files = [f for f in os.listdir(input_pcp_dir) if f.endswith('.tif') and f.startswith('L1_PCP_')]

    # Extract years from file names
    years = list(set([f[7:9] for f in pcp_files]))

    for year in tqdm(years):
        # Get all files for this year
        yearly_files = [f for f in pcp_files if f[7:9] == year]

        # Read and aggregate data, handling NoData values
        aggregated_data = None
        for file in yearly_files:
            file_path = os.path.join(input_pcp_dir, file)
            with rio.open(file_path) as src:
                data = src.read(1)

                # Convert NoData values to zero before aggregation
                nodata = src.nodata  # Get the NoData value from the source file
                if nodata is not None and np.isfinite(nodata):  # Check if a NoData value is specified and it is a finite number
                    data[data == nodata] = 0

                # Aggregate the data
                if aggregated_data is None:
                    aggregated_data = data
                else:
                    aggregated_data += data

        # Save aggregated data to new file
        output_file = os.path.join(output_pcp_a_dir, f'L1_PCP_A_{year}.tif')
        with rio.open(os.path.join(input_pcp_dir, yearly_files[0])) as src:
            meta = src.meta
            with rio.open(output_file, 'w', **meta) as dst:
                dst.write(aggregated_data, 1)

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


def apply_area_weighting(input_dir, output_dir_suffix='_AREA'):
    area_raster_file = "AETI_resampler_area.tif"
    resampler_file = "AETI_resampler.tif"

    # Load and resample the area raster
    with rio.open(area_raster_file) as area_src, rio.open(resampler_file) as resampler_src:
        transform, width, height = calculate_default_transform(
            area_src.crs, resampler_src.crs, area_src.width, area_src.height, *area_src.bounds)
        resampled_area = np.empty((height, width), dtype=np.float32)
        reproject(
            source=rio.band(area_src, 1), destination=resampled_area,
            src_transform=area_src.transform, src_crs=area_src.crs,
            dst_transform=transform, dst_crs=resampler_src.crs,
            resampling=Resampling.bilinear)

    # Create output directory if it doesn't exist
    output_dir = input_dir + output_dir_suffix
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Process each raster file in the input directory
    for filename in tqdm(os.listdir(input_dir)):
        if filename.endswith(".tif"):
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_dir, filename)  # Keep the original filename

            with rio.open(input_file) as src:
                data = src.read(1) * resampled_area  # Multiplying by the resampled area raster
                data = data.astype("float32")  # Keeping as float32 for precision
                data[data < 0] = 0  # Set all negative values to 0

                # Update metadata to reflect the change in datatype
                meta = src.meta
                meta.update(dtype='float32')

                # Check if the directory exists before writing, if not create it
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                
                with rio.open(output_file, 'w', **meta) as dst:
                    dst.write(data, 1)

# Function to extract land use data from raster data
def extract_landuse_value(landuse_raster, weather_raster, land_usage):
    
    weather_raster = weather_raster.assign_coords({"x": landuse_raster.x, "y": landuse_raster.y, })
    clipped_output = weather_raster.where(landuse_raster == land_usage)
    clipped_output.rio.write_nodata(input_nodata=-9999, encoded=True, inplace=True)
    return clipped_output

# Function to clip raster data
def get_raster_clip(raster, country_bound):
    with riox.open_rasterio(raster, chunks='auto', cache=False, masked=True, lock=False).rio.clip(
            country_bound.geometry.values, country_bound.crs, from_disk=True) as raster_output:
        return raster_output

# Function to generate a dataframe containing total raster data values
def get_raster_values(array_values, raster_name, country, year, land_usage, data_type):
    columns = ['country', 'landuse', 'year', 'filename', 'annual_sum', 'data_type']
    data_df = pd.DataFrame(columns=columns)
    
    # Filter out negative values
    array_values = array_values.where(array_values >= 0)
    
    # Use area_array to weight the sum
    annual_sum = (array_values).sum(skipna=True)
    
    annual_sum = np.array(annual_sum)
    print(annual_sum)

    data_df = data_df.append(
        pd.DataFrame([[country, land_usage, year, raster_name, annual_sum, data_type]], columns=columns),
        ignore_index=True)

    # Clear memory
    del annual_sum
    del array_values
    gc.collect()
    
    return data_df
    
# Generate the individual CSV files
def get_raster_data(raster_data_dir, raster_start, data_type, landuse_data_dir):
    """
    Generate raster values per each landuse per country per month.
    """
    columns = ['country', 'landuse', 'year', 'filename', 'annual_sum', 'data_type']
    landuses_df = pd.DataFrame(columns=columns)
    
    years = [x for x in range(2010, 2021)]
    ybar = tqdm(years)
    
    country_list = list(country_names.ROMNAM)
    cbar = tqdm(country_list)
    
    land_usage_list = [42]
    
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


           
def resample_area_raster():
    area_raster_file= "AETI_resampler_area.tif"
    resampler_file="AETI_resampler.tif"
    # Create output directory if it doesn't exist
    output_dir = 'AREA_RASTER'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Output file path
    output_file = os.path.join(output_dir, 'resampled_area.tif')
    
    # Resample the area raster file using the resampler file
    with rio.open(area_raster_file) as area_src, rio.open(resampler_file) as resampler_src:
        transform, width, height = rio.warp.calculate_default_transform(
            area_src.crs, resampler_src.crs, area_src.width, area_src.height, *area_src.bounds)
        resampled_area = np.empty((height, width), dtype=np.float32)
        rio.warp.reproject(
            source=rio.band(area_src, 1),
            destination=resampled_area,
            src_transform=area_src.transform,
            src_crs=area_src.crs,
            dst_transform=transform,
            dst_crs=resampler_src.crs,
            resampling=Resampling.bilinear)
    
    # Save the resampled area raster to the output directory
    meta = area_src.meta
    meta.update({
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'transform': transform,
        'crs': resampler_src.crs
    })
    
    with rio.open(output_file, 'w', **meta) as dst:
        dst.write(resampled_area, 1)

def extract_landuse_area():
    landuse_dir = 'LCC_A'
    area_raster_file = 'AREA_RASTER/resampled_area.tif'
    output_dir = 'LCC_A_AREA'
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with rio.open(area_raster_file) as area_src:
        area_data = area_src.read(1)

    # Processing each land use raster file in the land use directory
    for filename in tqdm(os.listdir(landuse_dir), desc="Processing Land Use Rasters"):
        if filename.endswith(".tif"):
            landuse_file = os.path.join(landuse_dir, filename)
            output_file = os.path.join(output_dir, filename.replace(".tif", "_AREA.tif"))

            with rio.open(landuse_file) as landuse_src:
                landuse_data = landuse_src.read(1)
                nodata = landuse_src.nodata

                # Setting nodata values and values not matching code 42 to 0
                mask = (landuse_data == 42)
                extracted_area_data = np.where(mask, area_data, 0)

                # Create a new raster file with the extracted area data
                meta = landuse_src.meta.copy()
                meta['dtype'] = 'float32'  # Change dtype to float32 to accommodate the area data

                with rio.open(output_file, 'w', **meta) as dst:
                    dst.write(extracted_area_data.astype(np.float32), 1)


            
# Aggregate the individual CSV files
def aggregate_csvs(aeti_file, pe_file, etb_file, area_file, gbwp_file, output_file):
    aeti_df = pd.read_csv(aeti_file)
    #pcp_df = pd.read_csv(pcp_file)
    pe_df = pd.read_csv(pe_file)
    etb_df = pd.read_csv(etb_file)
    area_df = pd.read_csv(area_file)
    gbwp_df = pd.read_csv(gbwp_file)
    

    aeti_df = aeti_df[['country', 'year', 'aeti']].rename(columns={'aeti': 'aeti'})
    #pcp_df = pcp_df[['country', 'year', 'pcp']].rename(columns={'pcp': 'pcp'})
    pe_df = pe_df[['country', 'year', 'pe']].rename(columns={'pe': 'pe'})
    etb_df = etb_df[['country', 'year', 'Etb']].rename(columns={'Etb': 'etb'})
    area_df = area_df[['country', 'year', 'area']].rename(columns={'area': 'area'})
    gbwp_df = gbwp_df[['country','year', 'gbwp']].rename(columns={'gbwp': 'gbwp'})
    

    aggregated_df = aeti_df.merge(pe_df, on=['country', 'year'])
    #aggregated_df = aggregated_df.merge(pcp_df, on=['country', 'year'])
    aggregated_df = aggregated_df.merge(etb_df, on=['country', 'year'])
    aggregated_df = aggregated_df.merge(area_df, on=['country', 'year'])
    aggregated_df = aggregated_df.merge(gbwp_df, on=['country', 'year'])
    

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

    ## Filter the aggregated yearly data to keep only landuse 42
    #aggregated_yearly_df = aggregated_yearly_df[aggregated_yearly_df['landuse'] == 'lcc_42']

    # Load the 'mycsv_new.csv' data
    mycsv_df = pd.read_csv(mycsv_file)

    # Merge the dataframes based on 'country'/'country_name' and 'year'
    merged_df = mycsv_df.merge(aggregated_yearly_df, left_on=['country', 'year'], right_on=['country', 'year'])

    # Select the desired columns from the merged dataframe
    selected_columns = [
        'country', 'year', 'GVAa','1-Cr', 'aeti','pe', 'etb','area', 'gbwp', 'ISO-3' 
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
        lambda row: (row['aeti'] / 1000) * 1000000, axis=1
    )
    merged_df['pe_m3'] = merged_df.apply(
        lambda row: (row['pe'] / 1000) * 1000000, axis=1
    )
    merged_df['area'] = merged_df.apply(
        lambda row: (row['area'] ), axis=1
    )
    merged_df['gbwp'] = merged_df.apply(
        lambda row: (row['gbwp']), axis=1
    )
    merged_df['etb_m3'] = merged_df.apply(
        lambda row: (row['etb'] / 1000) * 1000000, axis=1
    )
    merged_df['Awp'] = merged_df.apply(
        lambda row: max((row['GVAa'] * (row['1-Cr']) / row['etb']), 0) if row['etb'] > 0 else 0, axis=1
    )


    # Calculate tAwp
    merged_df['tAwp'] = 0
    for index, row in merged_df.iterrows():
        current_year = row['year']
        if current_year != 2010:
            previous_year = current_year - 1
            prev_row = merged_df[(merged_df['country'] == row['country']) & (merged_df['year'] == previous_year)]
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
        base_row = merged_df[(merged_df['country'] == row['country']) & (merged_df['year'] == base_year)]
        if not base_row.empty:
            base_awp = base_row.iloc[0]['Awp']
            if base_awp != 0:
                merged_df.at[index, 'cAwp'] = ((row['Awp'] - base_awp) / base_awp) * 100
            else:
                merged_df.at[index, 'cAwp'] = 0

    # Save the updated dataframe to a new CSV file
    merged_df.to_csv(output_file, index=False)




if __name__ == '__main__':

    #compute_lcc()
    #compute_aeti()
    #filter_aeti()     
    #compute_pcp() 

    #compute_pcp_a()
    #compute_pe()
    #compute_etb_m()     
    #compute_lcc()
    #resample_area_raster()
    #extract_landuse_area()
    #resample_area_raster()
    #calculate_landuse_area()
    # # area weighted gbwp,aeti,pcp,and pe raster files    
    # directories = ['GBWP_A_CORR', 'AETI_A_CORR', 'PCP_A_CORR', 'PE_A_CORR', 'Etb_A_CORR']   
    # for directory in directories:
        # apply_area_weighting(directory)
    

    # Generate the individual CSV files

    # # AETI Data Summation
    # aeti_df = get_raster_data(raster_data_dir='AETI_A_CORR_AREA', raster_start="L1_AETI_", data_type='AETI',
                              # landuse_data_dir='LCC_A')
    # aeti_df.to_csv('aeti_summation.csv')
    # del aeti_df
    # gc.collect()
    
    # # GBWP Data Summation
    # gbwp_df = get_raster_data(raster_data_dir='GBWP_A_CORR_AREA', raster_start="L1_GBWP_", data_type='GBWP',
                              # landuse_data_dir='LCC_A')
    # gbwp_df.to_csv('gbwp_summation.csv')
    # del gbwp_df
    # gc.collect()
    

    # # PCP Data Summation
    # pcp_df = get_raster_data(raster_data_dir='PCP_A_CORR_AREA', raster_start="L1_PCP_", data_type='PCP',
                            # landuse_data_dir='LCC_A')
    # pcp_df.to_csv('pcp_summation.csv')
    # del pcp_df
    # gc.collect()
    
    
    # # PE Data Summation
    # pe_df = get_raster_data(raster_data_dir='PE_A_CORR_AREA', raster_start="L1_PE_", data_type='PE',
                            # landuse_data_dir='LCC_A')
    # pe_df.to_csv('pe_summation.csv')
    # del pe_df
    # gc.collect()

    # # Etb Data Summation
    # etb_df = get_raster_data(raster_data_dir='Etb_A_CORR_AREA', raster_start="L1_Etb_", data_type='Etb',
                             # landuse_data_dir='LCC_A')
    # etb_df.to_csv('etb_summation.csv')
    # del etb_df
    # gc.collect()
    
    # # Area Data Summation
    # area_df = get_raster_data(raster_data_dir='LCC_A_CORR_AREA', raster_start="L1_LCC_", data_type='Area',
                             # landuse_data_dir='LCC_A')
    # area_df.to_csv('area_summation.csv')
    # del area_df
    # gc.collect()


    ## Aggregate the individual CSV files
    #aggregate_csvs('aeti_summation.csv', 'pe_summation.csv', 'etb_summation.csv','area_summation.csv','gbwp_summation.csv', 'aggregated_csvs.csv')

    # # Aggregate the values as yearly sums for each country
    # aggregate_yearly('aggregated_csvs.csv', 'aggregated_yearly.csv')


    # # Merge the data from 'aggregate_yearly.csv' with 'mycsv.csv'
    #merge_data('aggregated_csvs.csv', 'mycsv_1123.csv', 'merged_data.csv')


    # # Calculate the new attributes and save the results to 'awp_csv_n.csv'
    calculate_new_attributes('merged_data.csv', 'awp_csv_1123.csv')
