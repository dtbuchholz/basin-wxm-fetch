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

This project contains a simple setup wherein data pushed to Tableland [Basin](https://github.com/tablelandnetwork/basin-cli) (replicated to Filecoin) is fetched remotely and queried with [polars](https://www.pola.rs/).

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

1. Set up the virtual environment: `python -m venv env`
2. Source the `env` to use it: `source env/bin/activate`
3. Upgrade pip: `pip install --upgrade pip`
4. Install dependencies: `make install`

Or, step 1 and steps 3-4 can be done with `make venv` and `make install`; you just want to make sure you activate the environment in step 2 on your own. Note the core dependencies are `requests` and `polars`, and polars needs `aiohttp` and `fsspec` to work with remote files. The [`rich`](https://github.com/Textualize/rich) library is also used for logging.

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

Or, you can define a custom time range with `start` and `end` arguments (in milliseconds), which will be used to filter the data when _queries_ are executed.

```sh
make run start=1700438400000 end=1700783999000
```

This does not impact how Basin deals/data is fetched; _all_ publications and deals will be retrieved. Note: the timestamp range for the `xm_date` namespace stars on `1700438400000`. Once you run the command, it'll log information about the current status of each step in the run and the total time to complete upon finishing:

```sh
[18:57:25] INFO     Getting publications...done in 1.17s
[18:57:27] INFO     Getting deals for publications...done in 2.73s
[18:57:28] INFO     Forming remote URLs for deals...done in 0.68s
[18:59:07] INFO     Creating dataframe from remote files...done in 99.28s
[18:59:34] INFO     Executing queries...done in 26.32s
⠙ Writing results to files...
```

### Flags

The following flags are available for the `main.py` script:

- `--start`: Start timestamp (in ms) for the query (e.g., 1700438400000). Defaults to full range.
- `--end`: End timestamp (in ms) for the query (e.g., 1700783999000). Defaults to full range.
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
