import xarray as xr
import rioxarray
import numpy as np
from spectral.io import envi
import ujson
import base64

import fsspec

fs = fsspec.filesystem("file")

path = "test-data/ang20170323t202244_rdn_7000-7010.hdr"
dat = envi.open(path)

nsamp = int(dat.metadata["samples"])
nlines = int(dat.metadata["lines"])

def string_encode(x):
    bits = base64.b64encode(x)
    s = str(bits).lstrip("b'").rstrip("'")
    return f"base64:{s}"

waves = np.array(dat.metadata["wavelength"], np.float32)
waves_b64 = string_encode(waves)
# s2 = np.frombuffer(base64.b64decode(s), dtype=np.float32)

file_size = fs.du(path.rstrip(".hdr"))

output = {
  "version": 1,
  "refs": {
      ".zgroup": ujson.dumps({"zarr_format": 2}),
      ".zattrs": ujson.dumps({"Description": "Small chunk"}),
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
      "wavelength/0": waves_b64,
      "sample/.zarray": ujson.dumps({
          "chunks": [nsamp],
          "compressor": None,
          "dtype": "<f4",  # Float 32
          "fill_value": None,
          "filters": None,
          "order": "C",
          "shape": [nsamp],
          "zarr_format": 2
      }),
      "sample/.zattrs": ujson.dumps({
          "_ARRAY_DIMENSIONS": ["sample"]
      }),
      "sample/0": string_encode(np.arange(nsamp, dtype="<i4")),
      "line/.zarray": ujson.dumps({
          "chunks": [nlines],
          "compressor": None,
          "dtype": "<f4",  # Float 32
          "fill_value": None,
          "filters": None,
          "order": "C",
          "shape": [nlines],
          "zarr_format": 2
      }),
      "line/.zattrs": ujson.dumps({
          "_ARRAY_DIMENSIONS": ["line"]
      }),
      "line/0": string_encode(np.arange(nlines, dtype="<i4")),
      "radiance/.zarray": ujson.dumps({
          "chunks": [nlines, len(waves), nsamp],
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
      "radiance/0.0.0": [path.rstrip(".hdr"), 0, file_size]
  }
}

output_file = "test.json"
with fs.open(output_file, "w") as of:
    of.write(ujson.dumps(output))

# Test
dtest = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {
        "fo": output_file
    }
})
