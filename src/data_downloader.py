import os
import requests
import csv
import subprocess
from tqdm import tqdm
import shutil
import zipfile

# Install required packages
# !pip install flickrapi pandas

import flickrapi
import pandas as pd

# Flickr API credentials (replace with your own)
FLICKR_API_KEY = 'cc3e0318d184f3bdd2fbadc3ba7b11b0'
FLICKR_API_SECRET = 'c16cbee41a9c14ad'

# FMA Dataset URLs
FMA_METADATA_URL = 'https://os.unil.cloud.switch.ch/fma/fma_metadata.zip'
FMA_SMALL_URL = 'https://os.unil.cloud.switch.ch/fma/fma_small.zip'


def download_fma_data(music_dir):
    os.makedirs(music_dir, exist_ok=True)
    print("Downloading FMA Small dataset...")
    fma_zip_path = 'fma_small.zip'
    if not os.path.exists(fma_zip_path):
        with requests.get(FMA_SMALL_URL, stream=True) as r:
            r.raise_for_status()
            with open(fma_zip_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        print("FMA Small dataset downloaded.")
    else:
        print("FMA Small dataset already downloaded.")

    # Unzip the dataset
    print("Extracting FMA Small dataset...")
    with zipfile.ZipFile(fma_zip_path, 'r') as zip_ref:
        zip_ref.extractall('fma_small')
    print("FMA Small dataset extracted.")


def download_fma_metadata():
    metadata_zip_path = 'fma_metadata.zip'
    if not os.path.exists(metadata_zip_path):
        print("Downloading FMA metadata...")
        with requests.get(FMA_METADATA_URL, stream=True) as r:
            r.raise_for_status()
            with open(metadata_zip_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        print("FMA metadata downloaded.")
    else:
        print("FMA metadata already downloaded.")

    # Unzip the metadata
    print("Extracting FMA metadata...")
    with zipfile.ZipFile(metadata_zip_path, 'r') as zip_ref:
        zip_ref.extractall('fma_metadata')
    print("FMA metadata extracted.")


def get_track_genres():
    # Corrected file path
    tracks_csv_path = 'fma_metadata/fma_metadata/tracks.csv'
    # Read the tracks.csv file with proper headers
    tracks = pd.read_csv(tracks_csv_path, index_col=0, header=[0, 1])

    # Print columns for debugging
    print("Columns in tracks DataFrame:")
    print(tracks.columns)

    # Extract genres
    small = tracks[('set', 'subset')] == 'small'
    track_genres = tracks[small][[('track', 'genre_top')]]
    track_genres = track_genres.dropna()

    # Flatten the MultiIndex columns
    track_genres.columns = ['genre_top']

    return track_genres

def download_images(image_dir, track_genres):
    os.makedirs(image_dir, exist_ok=True)
    flickr = flickrapi.FlickrAPI(FLICKR_API_KEY, FLICKR_API_SECRET, format='parsed-json')

    print("Downloading images from Flickr...")
    for idx, (track_id, row) in enumerate(tqdm(track_genres.iterrows(), total=track_genres.shape[0])):
        genre = row['genre_top']
        # Search for images matching the genre
        try:
            photos = flickr.photos.search(
                text=genre,
                per_page=1,
                page=1,
                sort='relevance',
                content_type=1,
                safe_search=1,
                license='4,5,6,9,10',  # Creative Commons licenses
            )
            if photos['photos']['photo']:
                photo = photos['photos']['photo'][0]
                photo_id = photo['id']
                photo_info = flickr.photos.getSizes(photo_id=photo_id)
                # Get the largest available size
                sizes = photo_info['sizes']['size']
                url = sizes[-1]['source']
                img_data = requests.get(url).content
                with open(f"{image_dir}/image_{track_id}.jpg", 'wb') as handler:
                    handler.write(img_data)
            else:
                print(f"No image found for genre '{genre}'.")
        except Exception as e:
            print(f"Failed to download image for track {track_id}: {e}")


def download_and_prepare_data(image_dir='data/images', music_dir='data/music'):
    # Step 1: Download FMA Small dataset
    download_fma_data(music_dir)

    # Step 2: Download FMA metadata
    download_fma_metadata()

    # Step 3: Get track genres
    track_genres = get_track_genres()

    # Step 4: Download images based on genres
    download_images(image_dir, track_genres)

    # Step 5: Organize music files
    organize_music_files(music_dir, track_genres)


def organize_music_files(music_dir, track_genres):
    # The FMA music files are organized in subdirectories based on track IDs
    print("Organizing music files...")
    source_music_dir = 'fma_small'
    for track_id in track_genres.index:
        track_id_str = f"{track_id:06d}"
        subdir = track_id_str[:3]
        src_path = os.path.join(source_music_dir, subdir, f"{track_id_str}.mp3")
        dest_path = os.path.join(music_dir, f"music_{track_id}.mp3")
        if os.path.exists(src_path):
            os.makedirs(music_dir, exist_ok=True)
            shutil.move(src_path, dest_path)
        else:
            print(f"Music file for track {track_id} not found.")

    # Optionally, remove the extracted fma_small directory to save space
    if os.path.exists(source_music_dir):
        shutil.rmtree(source_music_dir)
        print("Removed temporary FMA small directory.")


if __name__ == "__main__":
    download_and_prepare_data()
