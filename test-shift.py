import xarray as xr
import rioxarray

dtest = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {
        "fo": "s3://dh-shift-curated/aviris/v1/gridded/zarr.json"
    }
})

dsub = dtest.isel(x=5000,y=5000)
%time dsubv = dsub.reflectance.values

dat = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {"fo": "s3://dh-shift-curated/aviris/v1/gridded/20220224_box_rfl_phase.json"}
})

dat.y.max()
dat.y.min()

dsub = dat.sel(x = slice(730000, 730500), y = slice(3810500, 3810000))
%time dvals = dsub.reflectance.values


import rasterio as rio

dat.attrs["map info"]

dat2 = dat
dat2 = dat2.rio.set_crs(32610)
dat2 = dat2.rio.transfo

# Sample --> X
# Line --> Y
drxr = xr.open_dataset("s3://dh-shift-curated/aviris/v1/gridded/20220224_box_rfl_phase", engine="rasterio")

drxr.spatial_ref.attrs

drxr.y.min().values
ycoords.min()
drxr.y.max().values
ycoords.max()

drxr.x.min().values
xcoords.min()
drxr.x.max().values
xcoords.max()

drxr.spatial_ref
