import numpy as np
import fsspec
import ujson
import os
import re
import datetime

import pyproj

from utils import read_envi_header, string_encode, zarray_common, envi_dtypes

def kerchunk_shift_rfl(rfl_path, output_file=None):
    if output_file is None:
        output_file = rfl_path.replace(".hdr", ".json")

    assert rfl_path.endswith(".hdr"), f"Need path to HDR file, not binary. Got {rfl_path}."
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

    fwhm = np.array(rfl_meta["fwhm"], np.float32)
    fwhm_b64 = string_encode(fwhm)
    fwhm_dict = {
        "fwhm/.zarray": ujson.dumps({
            **zarray_common,
            "chunks": [len(waves)],
            "dtype": "<f4",
            "shape": [len(waves)]
        }),
        "fwhm/.zattrs": ujson.dumps({
            "_ARRAY_DIMENSIONS": ["wavelength"]
        }),
        "fwhm/0": fwhm_b64
    }

    # Parse map information to generate X,Y coordinates
    proj_name = rfl_meta["map info"][0]
    ref_x = int(rfl_meta["map info"][1])
    ref_y = int(rfl_meta["map info"][2])
    px_easting = float(rfl_meta["map info"][3])
    px_northing = float(rfl_meta["map info"][4])
    x_size = float(rfl_meta["map info"][5])
    y_size = float(rfl_meta["map info"][6])
    utm_zone = rfl_meta["map info"][7]
    north_south = rfl_meta["map info"][8]
    datum = rfl_meta["map info"][9]

    # Project line/sample into X/Y using projection information
    lines = np.arange(nlines)
    ycoords = px_northing - y_size * (lines + 0.5)
    samples = np.arange(nsamp)
    xcoords = px_easting + x_size * (samples + 0.5)

    y_b64 = string_encode(ycoords)
    y_dict = {
        "y/.zarray": ujson.dumps({
            **zarray_common,
            "chunks": [nlines],
            "dtype": ycoords.dtype.str,
            "shape": [nlines],
        }),
        "y/.zattrs": ujson.dumps({
            "_ARRAY_DIMENSIONS": ["y"]
        }),
        "y/0": y_b64
    }

    x_b64 = string_encode(xcoords)
    x_dict = {
        "x/.zarray": ujson.dumps({
            **zarray_common,
            "chunks": [nsamp],
            "dtype": xcoords.dtype.str,
            "shape": [nsamp],
        }),
        "x/.zattrs": ujson.dumps({
            "_ARRAY_DIMENSIONS": ["x"]
        }),
        "x/0": x_b64
    }

    crs_string = ",".join(rfl_meta["coordinate system string"])
    crs = pyproj.CRS(crs_string)
    spref_dict = {
        "spatial_ref/.zarray": ujson.dumps({
            **zarray_common,
            "chunks": [],
            "dtype": np.dtype(np.int64).str,
            "shape": []
        }),
        "spatial_ref/.zattrs": ujson.dumps({
            **crs.to_cf(),
            "spatial_ref": crs_string,
            "GeoTransform": f"{px_easting} {x_size:.1f} -0.0 {px_northing} -0.0 -{y_size:.1f}",
            "_ARRAY_DIMENSIONS": []
        }),
        "spatial_ref/0": string_encode(np.int64(0))
    }

    rfl_data = rfl_path.rstrip(".hdr")
    byte_order = {"0": "<", "1": ">"}[rfl_meta["byte order"]]
    rfl_dtype = envi_dtypes[rfl_meta["data type"]].newbyteorder(byte_order)
    rfl_interleave = rfl_meta["interleave"]
    assert rfl_interleave == "bil", f"Interleave {rfl_interleave} unsupported. Only BIL interleave currently supported."
    ra = rfl_dtype.alignment
    reflectance_chunks = {
        f"reflectance/{i}.0.0": [rfl_data, i*nsamp*len(waves)*ra, nsamp*len(waves)*ra] for i in range(nlines)
    }
    reflectance_dict = {
        "reflectance/.zarray": ujson.dumps({
            **zarray_common,
            "chunks": [1, len(waves), nsamp],
            "dtype": rfl_dtype.str,
            "shape": [nlines, len(waves), nsamp],
        }),
        "reflectance/.zattrs": ujson.dumps({
            "_ARRAY_DIMENSIONS": ["y", "wavelength", "x"]
        }),
        **reflectance_chunks
    }

    output = {
        "version": 1,
        "refs": {
            ".zgroup": ujson.dumps({"zarr_format": 2}),
            ".zattrs": ujson.dumps({**rfl_meta}),
            **waves_dict, **x_dict, **y_dict,
            **spref_dict,
            **reflectance_dict
        }
    }

    with fsspec.open(output_file, "w") as of:
        of.write(ujson.dumps(output, indent=2))

    return output_file

# rfl_path = "s3://dh-shift-curated/aviris/v1/gridded/20220224_box_rfl_phase.hdr"
################################################################################

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description = "Create Kerchunk metadata for SHIFT reflectance mosaics")
    parser.add_argument("rfl_path", metavar="Reflectance HDR path", type=str,
                        help = "Path or S3 URL to radiance HDR file.")
    parser.add_argument("--output_file", metavar="Outut JSON path", type=str, default=None,
                        help = "Path to target output JSON file.")

    args = parser.parse_args()
    output_file = kerchunk_shift_rfl(args.rfl_path, args.output_file)
    print(f"Successfully created output file {output_file}.")
