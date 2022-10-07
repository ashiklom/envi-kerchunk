# Create Kerchunk JSON from ENVI binary

Create [kerchunk](https://github.com/fsspec/kerchunk)-style JSON metadata for ENVI binary files, allowing these files to be read by the Zarr library.

**NOTE: This repository is being very actively developed. Expect things to break and functionality/interfaces to change rapidly.**

## Usage

```python
# Create kerchunk metadata
from make_envi_kerchunk import make_envi_kerchunk

rdn_path = "test-data/ang20170323t202244_rdn_7000-7010.hdr"
loc_path = rdn_path.replace("_rdn_", "_loc_")
obs_path = rdn_path.replace("_rdn_", "_obs_")

output_file = make_envi_kerchunk(rdn_path, loc_path, obs_path, "output.json")

# Read ENVI file as Zarr
import xarray as xr
dtest = xr.open_dataset("reference://", engine="zarr", backend_kwargs={
    "consolidated": False,
    "storage_options": {
        "fo": "test-cli.json"
    }
})
```

Or use it from the command line:

```bash
python make_envi_kerchunk.py test-data/ang20170323t202244_rdn_7000-7010.hdr test-cli.json
```
