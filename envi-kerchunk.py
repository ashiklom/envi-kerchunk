import xarray as xr
import rioxarray
import numpy as np
from spectral.io import envi
import ujson
import base64

import fsspec

fs = fsspec.filesystem("file")

rdn_path = "test-data/ang20170323t202244_rdn_7000-7010.hdr"
obs_path = rdn_path.replace("_rdn_", "_obs_")
loc_path = rdn_path.replace("_rdn_", "_loc_")
assert fs.exists(rdn_path)
assert fs.exists(obs_path)
assert fs.exists(loc_path)

dat = envi.open(rdn_path)
obs = envi.open(obs_path)
loc = envi.open(loc_path)

nsamp = int(dat.metadata["samples"])
nlines = int(dat.metadata["lines"])

def string_encode(x):
    bits = base64.b64encode(x)
    s = str(bits).lstrip("b'").rstrip("'")
    return f"base64:{s}"

waves = np.array(dat.metadata["wavelength"], np.float32)
waves_b64 = string_encode(waves)

waves_dict = {
    "wavelength/.zarray": ujson.dumps({
        "chunks": [len(waves)],
        "compressor": None,
        "dtype": "<f4",  # Float 32
        "fill_value": None,
        "filters": None,
        "order": "C",
        "shape": [len(waves)],
        "zarr_format": 2
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
        "chunks": [nlines],
        "compressor": None,
        "dtype": "<i4",
        "fill_value": None,
        "filters": None,
        "order": "C",
        "shape": [nlines],
        "zarr_format": 2
    }),
    "line/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["line"]
    }),
    "line/0": lines_b64,
}

samps = np.arange(nsamp, dtype="<i4")
samps_b64 = string_encode(samps)
samps_dict = {
    "sample/.zarray": ujson.dumps({
        "chunks": [nsamp],
        "compressor": None,
        "dtype": "<i4",
        "fill_value": None,
        "filters": None,
        "order": "C",
        "shape": [nsamp],
        "zarr_format": 2
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
        "chunks": [1, len(waves), nsamp],
        "compressor": None,
        "dtype": "<f4",  # < = Byte order 0; f4 = data type 4
        "fill_value": None,
        "filters": None,
        "order": "C",
        "shape": [nlines, len(waves), nsamp],
        "zarr_format": 2
    }),
    "radiance/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["line", "wavelength", "sample"]
    }),
    **radiance_chunks
}

output = {
  "version": 1,
  "refs": {
      ".zgroup": ujson.dumps({"zarr_format": 2}),
      ".zattrs": ujson.dumps({"Description": "Small chunk"}),
      **waves_dict, **samps_dict, **lines_dict, **radiance_dict
  }
}

output_file = "test.json"
with fs.open(output_file, "w") as of:
    of.write(ujson.dumps(output, indent=2))

# Test
dtest = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {
        "fo": output_file
    }
})

from matplotlib import pyplot as plt
dtest.sel(line=5, sample=3).radiance.plot(); plt.show()
