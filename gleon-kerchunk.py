import numpy as np
import fsspec

import ujson

from utils import read_envi_header, string_encode, zarray_common, envi_dtypes

rfl_path = "s3://gleon-west2-shared/ang20160831t201002_rfl_v1n2/ang20160831t201002_corr_v1n2_img.hdr"
output_file = rfl_path.replace(".hdr", ".json")

with fsspec.open(rfl_path, "r") as f:
    rfl_meta = read_envi_header(f)

nsamp = int(rfl_meta["samples"])
nlines = int(rfl_meta["lines"])

waves = np.array(rfl_meta["wavelength"], np.float32)
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

rfl_data = rfl_path.rstrip(".hdr")
rfl_byte_order = {"0": "<", "1": ">"}[rfl_meta["byte order"]]
rfl_dtype = envi_dtypes[rfl_meta["data type"]].newbyteorder(rfl_byte_order)
rdn_interleave = rfl_meta["interleave"]
assert rdn_interleave == "bil", f"Interleave {rdn_interleave} unsupported. Only BIL interleave currently supported."
ra = rfl_dtype.alignment
reflectance_chunks = {
    f"reflectance/{i}.0.0": [rfl_data, i*nsamp*len(waves)*ra, nsamp*len(waves)*ra] for i in range(nlines)
}
reflectance_dict = {
    "reflectance/.zarray": ujson.dumps({
        **zarray_common,
        "chunks": [1, len(waves), nsamp],
        "dtype": rfl_dtype.str,  # < = Byte order 0; f4 = data type 4
        "shape": [nlines, len(waves), nsamp],
    }),
    "reflectance/.zattrs": ujson.dumps({
        "_ARRAY_DIMENSIONS": ["line", "wavelength", "sample"]
    }),
    **reflectance_chunks
}

output = {
    "version": 1,
    "refs": {
        ".zgroup": ujson.dumps({"zarr_format": 2}),
        ".zattrs": ujson.dumps({**rfl_meta}),
        **waves_dict, **samps_dict, **lines_dict,
        **reflectance_dict
    }
}

with fsspec.open(output_file, "w") as of:
    of.write(ujson.dumps(output, indent=2))

## Test that the output can be read
import xarray as xr
dat = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {"fo": output_file}
})

dat.isel(sample=300, line=3500).reflectance.values
