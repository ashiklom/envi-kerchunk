import numpy as np
import fsspec

import ujson
import base64

from read_envi_header import read_envi_header

def string_encode(x):
    bits = base64.b64encode(x)
    s = str(bits).lstrip("b'").rstrip("'")
    return f"base64:{s}"

def make_envi_kerchunk(rdn_path, loc_path, obs_path, output_file):

    # ENVI header description: 
    # https://www.l3harrisgeospatial.com/docs/enviheaderfiles.html

    zarray_common = {
        "compressor": None,
        "fill_value": None,
        "filters": None,
        "order": "C",
        "zarr_format": 2
    }

    envi_dtypes = {
        "1": np.dtype("int8"),
        "2": np.dtype("int16"),
        "3": np.dtype("int32"),
        "4": np.dtype("float32"),     # float32
        "5": np.dtype("float64"),     # float64
        "6": np.dtype("complex64"),     # complex64
        "9": np.dtype("complex128"),   # complex128
        "12": np.dtype("uint16"),
        "13": np.dtype("uint32"),
        "14": np.dtype("int64"),
        "15": np.dtype("uint64")
    }

    # assert fsi.exists(rdn_path)
    # assert fsi.exists(obs_path)
    # assert fsi.exists(loc_path)

    # obs = envi.open(obs_path)

    assert rdn_path.endswith(".hdr"), f"Need path to radiance HDR file, not binary. Got {rdn_path}."
    assert loc_path.endswith(".hdr"), f"Need path to location HDR file, not binary. Got {loc_path}."
    # assert obs_path.endswith(".hdr"), f"Need path to observation HDR file, not binary. Got {obs_path}."
    with fsspec.open(rdn_path, "r") as f:
        rdn_meta = read_envi_header(f)

    nsamp = int(rdn_meta["samples"])
    nlines = int(rdn_meta["lines"])

    waves = np.array(rdn_meta["wavelength"], np.float32)
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
    rdn_byte_order = {"0": "<", "1": ">"}[rdn_meta["byte order"]]
    rdn_dtype = envi_dtypes[rdn_meta["data type"]].newbyteorder(rdn_byte_order)
    rdn_interleave = rdn_meta["interleave"]
    assert rdn_interleave == "bil", f"Interleave {rdn_interleave} unsupported. Only BIL interleave currently supported."
    ra = rdn_dtype.alignment
    radiance_chunks = {
        f"radiance/{i}.0.0": [rdn_data, i*nsamp*len(waves)*ra, nsamp*len(waves)*ra] for i in range(nlines)
    }
    radiance_dict = {
        "radiance/.zarray": ujson.dumps({
            **zarray_common,
            "chunks": [1, len(waves), nsamp],
            "dtype": rdn_dtype.str,  # < = Byte order 0; f4 = data type 4
            "shape": [nlines, len(waves), nsamp],
        }),
        "radiance/.zattrs": ujson.dumps({
            "_ARRAY_DIMENSIONS": ["line", "wavelength", "sample"]
        }),
        **radiance_chunks
    }

    # Location file
    with fsspec.open(loc_path, "r") as f:
        loc_meta = read_envi_header(f)
    loc_data = loc_path.rstrip(".hdr")
    loc_byte_order = {"0": "<", "1": ">"}[loc_meta["byte order"]]
    loc_dtype = envi_dtypes[loc_meta["data type"]].newbyteorder(loc_byte_order)
    loc_interleave = loc_meta["interleave"]
    assert loc_interleave == "bil", f"Interleave {loc_interleave} unsupported. Only BIL interleave currently supported."
    la = loc_dtype.alignment
    lat_chunks = {
        f"lat/{i}.0": [loc_data, (i*nsamp*3)*la, nsamp*la] for i in range(nlines)
    }
    lat_dict = {
        "lat/.zarray": ujson.dumps({
            **zarray_common,
            "chunks": [1, nsamp],
            "dtype": loc_dtype.str,
            "shape": [nlines, nsamp],
        }),
        "lat/.zattrs": ujson.dumps({
            "_ARRAY_DIMENSIONS": ["line", "sample"],
        }),
        **lat_chunks
    }

    lon_chunks = {
        f"lon/{i}.0": [loc_data, (i*nsamp*3 + nsamp)*la, nsamp*la] for i in range(nlines)
    }
    lon_dict = {
        "lon/.zarray": ujson.dumps({
            **zarray_common,
            "chunks": [1, nsamp],
            "dtype": loc_dtype.str,
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
            ".zattrs": ujson.dumps({"loc": {**loc_meta}, "rdn": {**rdn_meta}}),
            **waves_dict, **samps_dict, **lines_dict,
            **radiance_dict, **lat_dict, **lon_dict
        }
    }

    with fsspec.open(output_file, "w") as of:
        of.write(ujson.dumps(output, indent=2))

    return output_file

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description = "Create Kerchunk metadata for ENVI binary files")
    parser.add_argument("rdn_path", metavar="Radiance HDR path", type=str,
                        help = "Path to radiance HDR file.")
    parser.add_argument("output_file", metavar="Outut JSON path", type=str,
                        help = "Path to target output JSON file.")
    parser.add_argument("--loc_path", metavar="Location HDR path", type=str,
                        help = "Path to location HDR file.")
    parser.add_argument("--obs_path", metavar="Observation HDR path", type=str,
                        help = "Path to observation HDR file.")

    args = parser.parse_args()
    rdn_path = args.rdn_path
    loc_path = args.loc_path
    obs_path = args.obs_path
    if loc_path is None:
        loc_path = rdn_path.replace("rdn", "loc")
        print(f"Loc path not set. Assuming {loc_path}.")
    if obs_path is None:
        obs_path = rdn_path.replace("rdn", "obs")
        print(f"Obs path not set. Assuming {obs_path}.")
    output_file = make_envi_kerchunk(rdn_path, loc_path, obs_path, args.output_file)
    print(f"Successfully created output file {output_file}.")
