"""Module for passively collecting data from OpenSky using the OpenSky API."""

import argparse
import datetime
import logging
import time
import os
import numpy as np
from opensky_api import OpenSkyApi

parser = argparse.ArgumentParser()
parser.add_argument("--username", help="OpenSky Username")
parser.add_argument("--password", help="OpenSky Password")
parser.add_argument('--gzip', type=str)
parser.add_argument('--bbox', nargs='+', type=float)
args = parser.parse_args()

BBOX = tuple(args.bbox)
GZIP = args.gzip
USERNAME = args.username
PASSWORD = args.password

logging.debug(f'BBOX = {BBOX}\nGZIP = {GZIP}')

# counting the number of states collecting
COUNTER = 0

# hour represents the hour since the python script began, not the
# hour of the day
RECORD_HOUR = 0

# Time interval in seconds between API requests. Default: 5
# Note that script was written with this default in mind. Altering value may cause issues.
REQUESTINTERVAL = 15

# Number of requests at which the script will output data to a new savefile. Default: 720
# Note that script was written with this default in mind. Altering value may cause issues.
REQUESTSBETWEENSAVES = 240

def get_formatted_datetime() -> str:
    """Function returning the current datetime as string, formatted as Y/m/d H:M:S"""
    timeinfo = time.gmtime()
    formatted_datetime = time.strftime("%Y/%m/%d %H:%M:%S", timeinfo)
    return formatted_datetime

def get_datetime_parts() -> tuple[str, str, str, str, str, str]:
    """Function returning a tuple of strings representing parts of the current GMT datetime."""
    timeinfo = time.gmtime()
    datetime_parts = (
        time.strftime("%Y", timeinfo),
        time.strftime("%m", timeinfo),
        time.strftime("%d", timeinfo),
        time.strftime("%H", timeinfo),
        time.strftime("%M", timeinfo),
        time.strftime("%S", timeinfo)
    )
    return datetime_parts

cols = ['alert', 'altitude', 'callsign', 'geoaltitude', 'groundspeed', 'hour',
        'icao24', 'last_position', 'latitude', 'longitude', 'onground', 'spi',
        'squawk', 'timestamp', 'track', 'vertical_rate']

alert = []
# baro altitude
altitude = []
callsign = []
geoaltitude = []
groundspeed = []
hour = []
icao24 = []
last_position = []
latitude = []
longitude = []
onground = []
spi = []
squawk = []
timestamp = []
track = []
vertical_rate = []

while True:
    start = time.time()
    api = OpenSkyApi(username=USERNAME,password=PASSWORD)

    try:
        states = api.get_states(time_secs=time.time(),bbox=BBOX)
        logging.debug(states)
    except Exception:
        time.sleep(REQUESTINTERVAL)
        continue


    # collecting lat/lon data
    try:

        for s in states.states:
            latitude.append(s.latitude)
            longitude.append(s.longitude)
            geoaltitude.append(s.geo_altitude)
            altitude.append(s.baro_altitude)
            track.append(s.true_track)
            icao24.append(s.icao24)
            groundspeed.append(s.velocity)
            vertical_rate.append(s.vertical_rate)
            callsign.append(s.callsign)
            last_position.append(datetime.datetime.fromtimestamp(s.time_position))
            timestamp.append(datetime.datetime.fromtimestamp(s.last_contact))
            date_info = time.gmtime(s.last_contact)
            hour.append(datetime.datetime(
                            year = date_info.tm_year,
                            month = date_info.tm_mon,
                            day = date_info.tm_mday,
                            hour = date_info.tm_hour
                            )
                        )
            spi.append(s.spi)
            squawk.append(s.squawk)
            alert.append(False)
            onground.append(s.on_ground)



    # this will occur if there is no data in states
    except Exception:

        print(f'Error collecting data for {get_formatted_datetime()}')

        end = time.time()

        if not abs(end-start) > REQUESTINTERVAL:
            time.sleep(REQUESTINTERVAL-abs(end-start))
        continue



    end = time.time()

    # We only want to collect data at most every 5 seconds
    if not abs(end-start) > REQUESTINTERVAL:
        time.sleep(REQUESTINTERVAL-abs(end-start))

    COUNTER += 1
    logging.debug('COUNTER incremented: %s', COUNTER)


    ## ~ 1 hour worth of data: 3600 seconds / 5 seconds data collection
    ## This could be updated based on the start time.time() and the current time.time()
    ## to make this exactly 1 hour instead of ~ 1 hour
    if COUNTER == REQUESTSBETWEENSAVES:
        logging.debug('Saving because COUNTER == REQUESTSBETWEENSAVES')

        # convert data to a matrix of (N, 2)
        opensky_data = np.vstack([alert, altitude, callsign, geoaltitude, groundspeed,
                            hour, icao24, last_position, latitude, longitude,
                            onground, spi, squawk, timestamp, track, vertical_rate]).T

        logging.debug('Data to save:\n%s', opensky_data)

        alert = []
        # baro altitude
        altitude = []
        callsign = []
        geoaltitude = []
        groundspeed = []
        hour = []
        icao24 = []
        last_position = []
        latitude = []
        longitude = []
        onground = []
        spi = []
        squawk = []
        timestamp = []
        track = []
        vertical_rate = []

        logging.debug('Cleared opensky_data attributes.')

        # save data
        path_datetimes = get_datetime_parts()
        dir_name = f'data/{path_datetimes[0]}/{path_datetimes[1]}/{path_datetimes[2]}/'
        hr = path_datetimes[3]
        minute = path_datetimes[4]
        os.makedirs(name=dir_name, exist_ok=True)

        if GZIP:
            np.savetxt(f'{dir_name}{hr}.gz', opensky_data, fmt='%s', delimiter=",")
            logging.debug('Saved as gzip to %s%s.gz', dir_name, hr)

        else:
            if not os.path.isfile(path=f'{dir_name}{hr}.npy'):
                np.savez_compressed(file=f'{dir_name}{hr}.npz', data=opensky_data)
                logging.debug('Saved as npy to %s%s.npz', dir_name, hr)

            else:
                np.savez_compressed(file=f'{dir_name}{hr}_{minute}.npz', data=opensky_data)
                logging.debug('Saved as npz to %s%s_%s.npz', dir_name, hr, minute)

        RECORD_HOUR += 1
        COUNTER = 0

        print(f"Hours Processed: {RECORD_HOUR}")
