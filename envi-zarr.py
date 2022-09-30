import xarray as xr
import rioxarray
path = "test-data/ang20170323t202244_rdn_7000-7010.hdr"
dx = xr.open_dataset(path.rstrip(".hdr"), engine="rasterio")

dx.to_zarr("test.zarr")

dxtest = xr.open_dataset("test.zarr")
