import xarray as xr
import rioxarray
import numpy as np
from spectral.io import envi
import ujson
import base64

import fsspec

fs = fsspec.filesystem("file")

def string_encode(x):
    bits = base64.b64encode(x)
    s = str(bits).lstrip("b'").rstrip("'")
    return f"base64:{s}"

# output_file = "small-example.json"
output_file = "big-example.json"

zarray_common = {
    "compressor": None,
    "fill_value": None,
    "filters": None,
    "order": "C",
    "zarr_format": 2
}

# rdn_path = "test-data/ang20170323t202244_rdn_7000-7010.hdr"
rdn_path = "../sbg-uncertainty/data/isofit-test-data/medium_chunk/ang20170323t202244_rdn_7k-8k.hdr"
obs_path = rdn_path.replace("_rdn_", "_obs_")
loc_path = rdn_path.replace("_rdn_", "_loc_")
assert fs.exists(rdn_path)
assert fs.exists(obs_path)
assert fs.exists(loc_path)

dat = envi.open(rdn_path)
# obs = envi.open(obs_path)

nsamp = int(dat.metadata["samples"])
nlines = int(dat.metadata["lines"])

waves = np.array(dat.metadata["wavelength"], np.float32)
waves_b64 = string_encode(waves)

waves_dict = {
    "wavelength/.zarray": ujson.dumps({
        **zarray_common,
        "chunks": [len(waves)],
        "dtype": "<f4",  # Float 32
        "shape": [len(waves)],
    }),
    "wavelength/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["wavelength"]
    }),
    "wavelength/0": waves_b64
}

lines = np.arange(nlines, dtype="<i4")
lines_b64 = string_encode(lines)
lines_dict = {
    "line/.zarray": ujson.dumps({
        **zarray_common,
        "chunks": [nlines],
        "dtype": "<i4",
        "shape": [nlines],
    }),
    "line/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["line"]
    }),
    "line/0": lines_b64
}

samps = np.arange(nsamp, dtype="<i4")
samps_b64 = string_encode(samps)
samps_dict = {
    "sample/.zarray": ujson.dumps({
        **zarray_common,
        "chunks": [nsamp],
        "dtype": "<i4",
        "shape": [nsamp],
    }),
    "sample/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["sample"]
    }),
    "sample/0": samps_b64
}

rdn_data = rdn_path.rstrip(".hdr")
file_size = fs.du(rdn_data)
radiance_chunks = {
    f"radiance/{i}.0.0": [rdn_data, i*nsamp*len(waves)*4, nsamp*len(waves)*4] for i in range(nlines)
}
radiance_dict = {
    "radiance/.zarray": ujson.dumps({
        **zarray_common,
        "chunks": [1, len(waves), nsamp],
        "dtype": "<f4",  # < = Byte order 0; f4 = data type 4
        "shape": [nlines, len(waves), nsamp],
    }),
    "radiance/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["line", "wavelength", "sample"]
    }),
    **radiance_chunks
}

# Location file
loc = envi.open(loc_path)
loc_data = loc_path.rstrip(".hdr")
lat_chunks = {
    f"lat/{i}.0": [loc_data, (i*nsamp*3)*8, nsamp*8] for i in range(nlines)
}
lat_dict = {
    "lat/.zarray": ujson.dumps({
        **zarray_common,
        "chunks": [1, nsamp],
        "dtype": "<f8", # Data type 5 = 64-bit float
        "shape": [nlines, nsamp],
    }),
    "lat/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["line", "sample"]
    }),
    **lat_chunks
}

lon_chunks = {
    f"lon/{i}.0": [loc_data, (i*nsamp*3 + nsamp)*8, nsamp*8] for i in range(nlines)
}
lon_dict = {
    "lon/.zarray": ujson.dumps({
        **zarray_common,
        "chunks": [1, nsamp],
        "dtype": "<f8", # Data type 5 = 64-bit float
        "shape": [nlines, nsamp],
    }),
    "lon/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["line", "sample"]
    }),
    **lon_chunks
}

output = {
  "version": 1,
  "refs": {
      ".zgroup": ujson.dumps({"zarr_format": 2}),
      ".zattrs": ujson.dumps({"Description": "Small chunk"}),
      **waves_dict, **samps_dict, **lines_dict,
      **radiance_dict, **lat_dict, **lon_dict
  }
}

with fs.open(output_file, "w") as of:
    of.write(ujson.dumps(output, indent=2))

# Test
dtest = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {
        "fo": output_file
    }
})

# from matplotlib import pyplot as plt
# dtest.sel(line=5, sample=3).radiance.plot(); plt.show()
