from pathlib import Path
import time
import argparse

import pandas as pd
import datashader as ds
# import xarray as xr

def save_image(image: ds.tf.Image, dest_path):
    image.to_pandas().to_parquet(dest_path)


def load_image(path):
    df = pd.read_parquet(path)
    return ds.tf.Image(df)


def stack_images(input_dir, dest_path):
    
    image_path_list = list(Path(input_dir).glob("*.parquet"))
    print(f"Stacking {image_path_list}")
    
    start = time.perf_counter()
    images = [load_image(path) for path in image_path_list]
    print(f"Loading parquets as images: {time.perf_counter() - start}s")

    # try:
    #     images = xr.align(*images, join='exact')
    # except ValueError:
    #     raise ValueError('RGB inputs to the stack operation could not be aligned; '
    #         'ensure they share the same grid sampling.')
    
    start = time.perf_counter()
    stacked = ds.tf.stack(*images)
    print(f"Stacking images: {time.perf_counter() - start}s")

    start = time.perf_counter()
    stacked.to_pil().save(dest_path)
    print(f"Saving image: {time.perf_counter() - start}s")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", type=Path, help="The root directory containing plot parquet files")
    parser.add_argument("dest_path", type=Path, help="The destination path for the image")
    args = parser.parse_args()
    return args.input_dir, args.dest_path


def main():
    input_dir, dest_path = parse_arguments()
    stack_images(input_dir, dest_path)


if __name__ == "__main__":
    main()
