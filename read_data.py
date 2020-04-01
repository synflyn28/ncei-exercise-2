"""
This code reads in GHCN CSV data for a specified number of years and calculate
various aggregations of precipitation and temperature data.
"""

import ftplib
import numpy as np
import re

import pandas as pd


def tidy_frame(obs_df):
    """
    Return a tidy dataframe of GHCN data.

    Args:
        obs_df: GHCN dataframe to tidy

    Returns (pandas._DataFrame): Tidy DataFrame
    """

    obs_df.columns = ['id', 'date', 'obs_element', 'value', 'm_flag', 'q-flag', 's-flag', 'obs-time']

    pivot_table = obs_df.pivot_table(index=['id', 'date'], columns='obs_element', values='value')

    return pivot_table.reset_index()


def read_years(year_begin, year_end):
    """
    Read in CSV GHCN data from its FTP endpoint.
    Args:
        year_begin:
        year_end:

    Returns:

    """
    ftp_session = ftplib.FTP('ftp.ncdc.noaa.gov')
    ftp_session.login()
    ftp_session.cwd('/pub/data/ghcn/daily/by_year/')

    year_range = np.arange(year_begin, year_end + 1)
    file_listing = ftp_session.nlst('*.csv.gz')
    valid_dfs = []

    for csv_file in file_listing:
        csv_year = int(re.match('\d\d\d\d', csv_file).group(0))
        if csv_year in year_range:

            valid_dfs.append(
                pd.read_csv(
                    'ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{}'.format(csv_file),
                    header=None,
                )
            )

    ftp_session.close()
    valid_df = pd.concat(valid_dfs, axis=0)

    return tidy_frame(valid_df)


def main():
    """
    Main Function
    Returns: None
    """

    ghcn_df = read_years(1880, 1881)
    print(ghcn_df.head())


if __name__ == '__main__':
    main()
