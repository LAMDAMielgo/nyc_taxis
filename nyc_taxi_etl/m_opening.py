# -------------- files
import os
import zipfile
import requests
from io import BytesIO
import time

# -------------- for data
import numpy as np
import pandas as pd

# -------------------------------------------------------------------------------------------------------- FUNCTIONS FOR FILE LOADING

def request_info_from_ZIP(zipfile_dir):
    """
    input
    output
    """
    print(f"---------------------- Getting ZIP file")
    tic = time.perf_counter()

    response = requests.get(zipfile_dir, stream=True)
    f = BytesIO()
    f.write(response.content)

    toc = time.perf_counter()
    mins = (toc - tic) // 60;
    secs = np.around((toc - tic) % 60, 3)

    print(f"---------------------- Done in {mins}'{secs}''")
    return f

def getting_df_fromZip(zipfile_info, minLen_toDisgard):
    """
    input
    output
    """
    filenames, frame_to_concat = [], []
    # Open and concat df
    print(f"---------------------- Opening ZIP file and construction of DF \n");
    tic = time.perf_counter()

    with zipfile.ZipFile(zipfile_info) as zip_:
        for filename in zip_.namelist():
            if len(filename) > minLen_toDisgard:
                # there is a folder data/ with nothing inside
                # this is to only pick valid textfiles
                # based on their lenght
                with zip_.open(filename) as file_:
                    if len(filenames) == 0:
                        # for first file in data.zip
                        first_frame = pd.read_csv(file_, sep=',')
                        column_names = first_frame.columns.tolist()
                        filenames.append(filename);
                        frame_to_concat.append(first_frame)
                        memory_usage = first_frame.memory_usage(index=True, deep=False).sum() / (1000 * 1024)
                        print(
                            f"{filename} \tDone \t Memory Usage: {np.around(memory_usage, 2)} Mb \t Shape {first_frame.shape}")

                    else:
                        new_frame = pd.read_csv(file_, sep=',', names=column_names)
                        filenames.append(filename);
                        frame_to_concat.append(new_frame)
                        memory_usage += new_frame.memory_usage(index=True, deep=False).sum() / (1000 * 1024)

                        print(
                            f"{filename} \tDone \t Memory Usage: {np.around(memory_usage, 2)} Mb \t Shape {new_frame.shape}")

    toc = time.perf_counter();
    mins = (toc - tic) // 60;
    secs = np.around((toc - tic) % 60, 3)
    print(
        f"\n ---------------------- Done Running all in {mins}'{secs}'' \t Total frames {len(frame_to_concat)} return as {type(frame_to_concat)}")
    # returns list of list of DFs
    return frame_to_concat