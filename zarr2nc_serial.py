#!/usr/bin/env python
# coding: utf-8

import xarray as xr
import xbitinfo as xb
import fsspec
from dotenv import load_dotenv
from pathlib import Path

def zarr2nc(z):
    name = f'{Path(z).stem}.nc'
    local_file = f'/tmp/{name}'
    print(name)
    cloud_file = f'abfs://cmre-cc01/data/era5/{name}'
    ds = xr.open_dataset(z, engine='zarr', chunks={}, backend_kwargs=dict(consolidated=False), inline_array=True)
    ds.to_compressed_netcdf(local_file)
    cloud_fs.upload(local_file, cloud_file)
    local_fs.rm(local_file)


if __name__ == "__main__":
     
    import os
    if 'NEBARI_JUPYTERHUB_SSH_PORT' in os.environ.keys():   # NEBARI
        dot_env_file = '/shared/users/nebari-setup/lib/CMRE_Blob_Keys.env'
    elif 'AML_CloudName' in os.environ.keys():  # AzureML
        dot_env_file = '/home/azureuser/cloudfiles/code/Users/CMRE_Blob_Keys.env'
    else:
        print('unknown env')
    print(dot_env_file)
    _ = load_dotenv(dot_env_file)  # loads Azure credentials

#   cluster = LocalCluster(n_workers=2, threads_per_worker=2)
#   client = cluster.get_client()  
#    storage_options = {'connection_string': os.environ['AZURE_STORAGE_CONNECTION_STRING']}
    cloud_fs = fsspec.filesystem('abfs', anon=False, use_listings_cache=False, skip_instance_cache=True)
    local_fs = fsspec.filesystem('file')

    zlist = cloud_fs.glob("abfs://cmre-cc01/rsignell2/data/era5/*.zarr")
    zlist = [f'abfs://{z}' for z in zlist]

    # each conversion takes about 80GB of space on /tmp.    
    # the 4 GB instance has 500GB disk
#    _ = dask.compute(*[dask.delayed(zarr2nc)(z) for z in zlist], retries=10)
    
# Serial 
    _ = [zarr2nc(z) for z in zlist]

 