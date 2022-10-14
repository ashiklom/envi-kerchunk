from shift_kerchunk import kerchunk_shift_rfl
from utils import string_encode
import s3fs
import fsspec
import ujson
from kerchunk.combine import MultiZarrToZarr
import os
import re
import datetime
import numpy as np

s3 = s3fs.S3FileSystem(anon=False)

flist_all = s3.ls("s3://dh-shift-curated/aviris/v1/gridded/")

flist = sorted([f"s3://{f}" for f in flist_all if f.endswith("rfl_phase.hdr")])

json_list = [kerchunk_shift_rfl(of) for of in flist]

# Combined
def parse_date(fname):
    basename = os.path.basename(fname)
    dstring = re.match(r'\d{8}', basename)
    assert dstring
    date = datetime.datetime.strptime(dstring.group(), "%Y%m%d")
    return np.datetime64(date)

new_dims = np.array([parse_date(f) for f in json_list])
combined = MultiZarrToZarr(
    json_list,
    remote_protocol="s3",
    remote_options={'anon': True},
    coo_map={'time': new_dims},
    concat_dims=['time'],
    identical_dims=['x', 'y', 'wavelength']
)

combined_t = combined.translate()
with fsspec.open("s3://dh-shift-curated/aviris/v1/gridded/zarr.json", "w") as of:
    of.write(ujson.dumps(combined_t, indent=2))
