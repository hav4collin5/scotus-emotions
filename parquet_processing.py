import os
import pandas as pd
import concurrent.futures

# Configuration
max_workers = 4  # Number of concurrent workers
chunk_size = 100000  # Number of lines per chunk

# File paths
file_csv = 'opinions-2024-11-30.csv'  # Now reading the uncompressed CSV
folder_parquet_chunks = 'parquet_chunks'

# Ensure output directories exist
os.makedirs(folder_parquet_chunks, exist_ok=True)
os.makedirs('html', exist_ok=True)
os.makedirs('plain', exist_ok=True)

# CSV columns to grab
columns = ['id', 'plain_text', 'html']


def write_file(filename, data):
    """Safely writes data to a file."""
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(data)
    except Exception as e:
        print(f"Error writing {filename}: {e}")


def process_chunk(chunk, chunk_index):
    """Processes a single chunk: converts to Parquet and extracts text."""
    chunk_file = os.path.join(folder_parquet_chunks, f'chunk_{chunk_index}.parquet')
    
    try:
        # Save chunk as Parquet
        chunk.to_parquet(chunk_file, engine='pyarrow', compression='snappy')
        print(f"Saved chunk {chunk_index} to {chunk_file}")

        # Process the chunk immediately
        for _, row in chunk.iterrows():
            try:
                id = row['id']
                plain = row.get('plain_text', None)
                html = row.get('html', None)

                if pd.notna(plain):
                    write_file(f'plain/{id}.txt', plain)

                if pd.notna(html) and html.strip():
                    write_file(f'html/{id}.html', html)

                print(f"Processed opinion {id}")

            except Exception as e:
                print(f"Error processing opinion {row.get('id', 'UNKNOWN')}: {e}")

    except Exception as e:
        print(f"Error processing chunk {chunk_index}: {e}")


# Multi-threaded processing
chunk_index = 0
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = []

    for chunk in pd.read_csv(
        file_csv,
        delimiter=',',
        quotechar='`',
        chunksize=chunk_size,
        usecols=columns,
        on_bad_lines='skip'  # Skips corrupt lines instead of failing
    ):
        futures.append(executor.submit(process_chunk, chunk, chunk_index))
        chunk_index += 1
        print(f"Queued chunk {chunk_index}")

    for future in concurrent.futures.as_completed(futures):
        future.result()  # Report any errors
