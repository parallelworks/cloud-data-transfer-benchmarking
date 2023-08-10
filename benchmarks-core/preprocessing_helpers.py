from numcodecs import blosc, Blosc, gzip, bz2
import dask
import xarray as xr


def zarr_compression(algorithm='lz4', level=5):
    """Assigns a compressor to a variable. Default
    values of inputs are set to what Zarr compresses by default.

    Availale Compressors:
        - lz4
        - gzip
        - bzip2
        - blosclz
        - lz4hc
        - zlib
        - zstd
    """

    match algorithm:
        case "gzip":
            compressor = gzip.GZip(level=level)
        case "bzip2":
            compressor = bz2.BZ2(level=level)
        case other:
            compressor = Blosc(cname=algorithm, clevel=level)

    return compressor


def dataset_rechunk(ds, data_vars, chunksize):
    
    darrays = []
    for data_var in data_vars:
        chunksize_bytes = 1e6*chunksize

        coords = ds[data_var].dims
        if chunksize == float(0):
            chunk_list = ds[data_var].encoding['chunksizes']
        else:
            chunk_len = round((chunksize_bytes / ds[data_var].dtype.itemsize)**(1/len(coords)))
            chunk_list = [chunk_len] * len(coords)

        ds[data_var] = ds[data_var].chunk(chunks=dict(zip(coords, chunk_list)))

    # Create new dataset with only the user-specified data variables
    ds = xr.merge([ds[data_var] for data_var in data_vars])

    return ds