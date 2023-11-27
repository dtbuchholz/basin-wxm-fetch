# Tableland Basin + WeatherXM Demo

[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg)](https://github.com/RichardLitt/standard-readme)

> Compute queries for WeatherXM data pushed to Tableland Basin

## Table of Contents

- [Background](#background)
  - [Data](#data)
- [Install](#install)
- [Usage](#usage)
  - [Flags](#flags)
  - [Makefile Reference](#makefile-reference)
- [Contributing](#contributing)
- [License](#license)

## Background

This project runs a simple analysis on [WeatherXM](https://weatherxm.com/) data that's pushed to Tableland [Basin](https://github.com/tablelandnetwork/basin-cli) (replicated to Filecoin). It fetches the data remotely and queries it with [polars](https://www.pola.rs/).

### Data

The script fetches data from the WeatherXM's `xm_data` Basin namespace (created by `0xfc7C55c4A9e30A4e23f0e48bd5C1e4a865dA06C5`) on a cron schedule. For every run, it will query data and write the results to:

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

To set things up (for local development), you'll need to do the following:

1. Set up the virtual environment: `make venv`
2. Source the `env` to use it: `source env/bin/activate`
3. Upgrade pip and instal: `make install`
4. Install dependencies: `make install`

You want to make sure you activate the environment in the second step before installing. Note the core dependencies are `requests` and `polars`, and polars needs `aiohttp` and `fsspec` to work with remote files. The [`rich`](https://github.com/Textualize/rich) library is also used for logging.

Once you've done this, you'll also need to make sure the [Basin CLI](https://github.com/tablelandnetwork/basin-cli) is installed; it's part of the underlying application logic. You'll need [Go](https://go.dev/doc/install) installed to do this, and then run:

```sh
go install github.com/tablelandnetwork/basin-cli/cmd/basin@latest
```

## Usage

Running `src/main.py` will fetch remote files from Tableland Basin, load them into a `polars` dataframe, and then run queries on the data.

To use default time ranges (the full dataset), run:

```sh
make run
```

Or, you can define a custom time range with `start` and `end` arguments (Unix epoch timestamps in milliseconds), which will be used to filter the data when _queries_ are executed.

```sh
make run start=1700438400000 end=1700783999000
```

This does not impact how Basin deals/data is fetched; _all_ publications and deals will be retrieved. Note: the timestamp range for the `xm_data` namespace stars on `1700438400000`. Once you run the command, it'll log information about the current status of each step in the run and the total time to complete upon finishing:

```sh
[16:59:03] INFO     Getting publications...done in 1.25s
[16:59:05] INFO     Getting deals for publications...done in 2.08s
[16:59:06] INFO     Forming remote URLs for deals...done in 0.59s
[16:59:56] INFO     Creating dataframe from remote files...done in 50.25s
[17:00:26] INFO     Executing queries...done in 29.88s
⠙ Writing results to files...
```

### Flags

The following flags are available for the `main.py` script:

- `--start`: Start timestamp (in Unix ms) for the query (e.g., 1700438400000). Defaults to full range.
- `--end`: End timestamp (in Unix ms) for the query (e.g., 1700783999000). Defaults to full range.
- `--verbose`: Enable verbose logging to show stack traces for errors. Defaults to true.

### Makefile Reference

The following defines all commands available in the Makefile:

- `make install`: Install dependencies with `pip`, upgrading pip first.
- `make run`: Run the `main.py` program to fetch Basin/wxm data, run queries, and write to metric summary files.
- `make venv`: Create a virtual environment (only for local development).
- `make freeze`: Freeze dependencies (only needed if you make changes to the deps during local development).

## Contributing

PRs accepted.

Small note: If editing the README, please conform to the standard-readme specification.

## License

MIT AND Apache-2.0, © 2021-2023 Tableland Network Contributors
