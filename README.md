# rechunk-jobarray

Trying out [Coiled's SLURM-style job arrays](https://docs.coiled.io/blog/slurm-job-arrays.html).

Here we are trying out a rechunking workflow where we rechunk a large dataset in pieces using job array, generating a collection of rechunked zarr datasets.   

We submit the job to Coiled using the script `submit_coiled_batch.sh`, which simply does:
```
coiled batch run ./ERA5-rechunker-AWS.py
```
while all the details of the machine type specified, amount of disk, software environment, etc is stored in the submitted Python script as [SLURM-like params](https://github.com/OpenScienceComputing/rechunk-jobarray/blob/main/ERA5-rechunker-AWS.py#L4-L12). 
The S3 credentials to write to the Open Storage Network object storage are passed as secret environment variables along to Coiled. 

