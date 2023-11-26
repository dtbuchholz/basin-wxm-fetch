# Tableland Basin + WeatherXM Demo

[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg)](https://github.com/RichardLitt/standard-readme)

> Compute queries for wxm data pushed to Tableland Basin

## Table of Contents

- [Background](#background)
  - [Data](#data)
- [Install](#install)
- [Usage](#usage)
  - [Makefile Reference](#makefile-reference)
- [Contributing](#contributing)
- [License](#license)

## Background

This project contains a simple setup wherein data pushed to Tableland [Basin](https://github.com/tablelandnetwork/basin-cli) (replicated to Filecoin) is fetched remotely and queried with [polars](https://www.pola.rs/).

### Data

The script fetches data from the WeatherXM's `wxm2` Basin namespace on a cron schedule. For every run, it will query data and write the results to:

- [Data](./Data.md): Summary metrics for the run, including averages across all columns.
- [History](./history.csv): A CSV file containing the full history of all runs, along with the run date and time.

The schema for the data is as follows:

- `device_id` (varchar): Unique identifier for the device.
- `timestamp` (bigint): Timestamp (unix milliseconds).
- `temperature` (double): Temperature (Celsius).
- `humidity` (double): Relative humidity reading (%).
- `precipitation_accumulated` (double): Total precipitation (millimeters).
- `wind_speed` (double): Wind speed (meters per second).
- `wind_gust` (double): Wind gust (meters per second).
- `wind_direction` (double): Wind direction (degrees).
- `illuminance` (double): Illuminance (lux).
- `solar_irradiance` (double): Solar irradiance (watts per square meter).
- `fo_uv` (double): UV-related index value.
- `uv_index` (double): UV index.
- `precipitation_rate` (double): Precipitation rate (millimeters per hour).
- `pressure` (double): Pressure (HectoPascals).
- `model` (varchar): Model of the device (either WXM WS1000 or WXM WS2000).
- `name` (varchar): Name of the device.
- `cell_id` (varchar): Cell ID of the device.
- `lat` (double): Latitude of the cell.
- `lon` (double): Longitude of the cell.

Most of the columns above are included in the mean calculations, and there are three additional columns for aggregates:

- `total_precipitation` (double): Total `precipitation_accumulated` (millimeters).
- `num_devices` (int): Count of unique `device_id` values.
- `cell_mode` (varchar): Most common `cell_id` value.

## Install

To set things up on your machine, you'll need to do the following:

1. Set up the virtual environment: `python -m venv env`
2. Upgrade pip: `pip install --upgrade pip`
3. Source the `env` to use it: `source env/bin/activate`

Then, you can use the Makefile command to install dependencies: `make install`

Alternatively, you can do all of these in one step (for local development) with: `make setup`

Note the core dependencies are `requests` and `polars`, and polars needs `aiohttp` and `fsspec` to work with remote files. The [`rich`](https://github.com/Textualize/rich) library is also used for logging.

## Usage

Running `src/main.py` will fetch remote files from Tableland [Basin](https://github.com/tablelandnetwork/basin-cli), load them into a `polars` dataframe, and then run queries on the data.

To use default time ranges (the full dataset), run:

```sh
make run
```

Or, you can define a custom time range with `start` and `end` arguments (in milliseconds), which will be used to filter the data when _queries_ are executed.

```sh
make run start=1697328000000 end=1697932798895
```

This does not impact how Basin deals/data is fetched; _all_ publications and deals will be retrieved. Note: the timestamp range for the wxm data namespace is for Oct 15-21: `1697328000000` to `1697932798895`. Once you run the command, it'll log information about the current status of each step in the run and the total time to complete upon finishing:

```sh
[04:02:10] INFO     Getting publications...done in 0.96s
[04:02:19] INFO     Getting deals for publications...done in 8.96s
           INFO     Forming remote URLs for deals...done in 0.76s
[04:03:47] INFO     Creating dataframe from remote IPFS files...done in 87.51s
[04:03:48] INFO     Query range: 2023-10-15 00:00:00 to 2023-10-21 23:59:58
[04:05:11] INFO     Executing queries...done in 83.11s
⠙ Writing results to files...
```

### Makefile Reference

The following defines all commands available in the Makefile:

- `make install`: Install dependencies with `pip`.
- `make run`: Run the `main.py` program to fetch Basin/wxm data, run queries, and write to metric summary files.
- `make setup`: Create a virtual environment, upgrade pip, and source your `env` (only for local development).
- `make freeze`: Freeze dependencies (only needed if you make changes to the deps during local development).

## Contributing

PRs accepted.

Small note: If editing the README, please conform to the standard-readme specification.

## License

MIT AND Apache-2.0, © 2021-2023 Tableland Network Contributors
