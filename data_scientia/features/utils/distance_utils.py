#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from pyproj import Geod

# Distance will be measured on this ellipsoi
_WGS84_GEOD = Geod(ellps='WGS84')


def coord_dist(lat_a, long_a, lat_b, long_b):
    """Get distance between pairs of (latitude, longitude) points.

    Parameters
    ----------
    lat1 : list
        First list of latitude coordinates.
    lon1 : list
        First list of longitude coordinates.
    lat2 : list
        Second list of latitude coordinates.
    lon2 : list
        Second list of longitude coordinates.

    Returns
    -------
    distance : list
        Distance betweent the pair of coordinates.

    Example
    --------
    ::

        lat1 = pd.Series([19.4361])
        lon1 = pd.Series([-99.0719])

        inc = 1e-4
        lat2 = lat1 + inc
        lon2 = lon1 + inc

        dist = coord_dist(lat1, lon1, lat2, lon2)
    """

    az12, az21, dist = _WGS84_GEOD.inv(
        list(long_a), list(lat_a),
        list(long_b), list(lat_b))


    return dist


def degrees_minutes_seconds_to_decimal_degrees(degrees_minutes_seconds):
    """Convert coordinate from degrees minutes seconds notation ot decimal
    degrees.


    Parameters
    -----------
    degrees_minutes_seconds: str

    Returns
    --------
    decimal_degrees: float

    Examples
    ---------
    ::

        degrees_minutes_seconds = '21°52´47.362N"'
        degrees_minutes_seconds_to_decimal_degrees(degrees_minutes_seconds)
        Out[1]: 21.87982277777778

        degrees_minutes_seconds = '102°17´45.768W"'
        degrees_minutes_seconds_to_decimal_degrees(degrees_minutes_seconds)
        Out[2]: -102.29604666666667
    """
    split_degrees = degrees_minutes_seconds.split('°')
    degrees = split_degrees[0].lstrip("0")
    if len(degrees) > 0:
        degrees = eval(degrees)
    else:
        degrees = 0
    minute_seconds = split_degrees[1]

    split_minutes = minute_seconds.split('´')
    minutes = split_minutes[0].lstrip("0")
    if len(minutes) > 0:
        minutes = eval(minutes)
    else:
        minutes = 0

    seconds = split_minutes[1][:-2].lstrip("0")
    north_south_east_west = split_minutes[1][-2:-1]

    if seconds != '.':
        seconds = eval(seconds)
    else:
        seconds = 0

    decimal_degrees = degrees + (minutes / 60) + (seconds / 3600)

    if north_south_east_west in ['W', 'S']:
        decimal_degrees *= -1

    return decimal_degrees
