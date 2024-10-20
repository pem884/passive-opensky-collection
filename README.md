# Passive-OpenSky-Collection

Python 3.x repo for passively collecting data from OpenSky using the OpenSky API.

## Requirements

### OpenSky Account

You will need to obtain an OpenSky account for accessing their data to full capacity. Account registration can be found here: [Register](https://opensky-network.org/index.php?option=com_users&view=registration).

### OpenSky API

The OpenSky API python package will need to be cloned and installed from the [OpenSky Github](https://github.com/openskynetwork/opensky-api). Follow the recommended **pip** installation.

## Usage

`collectData.py` is simple script for passively collecting **real-time** data from OpenSky in a compatible format with the Python [Traffic](https://github.com/xoolive/traffic) module. The data is then saved as compressed numpy arrays.

The run script is as follows:
```python
python collectData.py --username your_username --password your_password
                      --bbox min_latitude max_latitude min_longitude max_longitude

```

This script will begin collecting data approximately every 5 seconds from OpenSky for the specified bounding box. Data will be collected for approximately 1 hour and then saved to a created data directory with the following format: `data/<year>/<month>/<day>/<hour>.npz`. `.npz` files are compressed numpy arrays which are much more memory efficient as compared to `.csv` and even `.npy` files. In addition, collecting data in 1 hour increments ensures that the python script does not consume too much memory. In the event that a file aleady exists for that hour, then the minute will also be added to the file name: `/<hour>_<min>.npy`.

Please note that the data corresponding to `<hour>` will be all of the data collected from the point of save to 1 hour in the past. Depending on when the script is started, this time may be over two hours. For example, if the script is started at 1430, the first data will be saved as `/13.npy` and incorporate the data from **1430 to 1330**.

To access the `.npz` data, you can use the following command,

```python
import numpy as np

data = np.load("path/to/file.npz",allow_pickle=True)['data']
```
## Example

Below are two example outputs from collecting 2 and 20 hours worth of data! Scripts to generate this plot are coming soon...

### 2 hours
![2 Hours](figures/2Hours.png)

### 20 hours
![20 Hours](figures/20Hours.png)

## Acknowledging

If you found this repository useful or interesting, please help by adding a star to help the visibility :)
