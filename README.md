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

This project runs a simple analysis on [WeatherXM](https://weatherxm.com/) data that's pushed to Tableland [Basin](https://github.com/tablelandnetwork/basin-cli) (replicated to Filecoin). It fetches remote data and queries it with [DuckDB](https://duckdb.org).

### Data

The script fetches data from the WeatherXM's `xm_data` Basin namespace (created by `0xfc7C55c4A9e30A4e23f0e48bd5C1e4a865dA06C5`) on a cron schedule with GitHub Actions. For every run, it will query data and write the results to:

- [Data](./Data.md): Summary metrics for the run, including averages across all columns.
- [History](./history.csv): A CSV file containing the full history of all runs, along with the run date and time.

## Install

To set things up (for local development), you'll need to do the following:

1. Set up the virtual environment: `make venv`
2. Source the `env` to use it: `source env/bin/activate`
3. Upgrade pip and instal: `make install`
4. Install dependencies: `make install`

You want to make sure you activate the environment in the second step before installing. Note the core dependencies installed are:

- [`duckdb`](https://duckdb.org/docs/api/python/overview): Creates an in-memory SQL database for querying parquet files extracted from Basin.
- [`polars`](https://pola.rs/): Used for DataFrame operations as part of post-query logic.
- [`pyarrow`](https://pypi.org/project/pyarrow/): Required for DuckDB to work with parquet files.
- [`rich`](https://github.com/Textualize/rich): Used for logging purposes.

Once you've done this, you'll also need to make sure the [Basin CLI](https://github.com/tablelandnetwork/basin-cli) is installed; it's part of the underlying application logic. You'll need [Go](https://go.dev/doc/install) 1.21 installed to do this, and then run:

```sh
go install github.com/tablelandnetwork/basin-cli/cmd/basin@latest
```

Also, the [`go-car`](https://github.com/ipld/go-car) CLI is required to extract the underlying parquet files from the CAR files retrieved from Basin. You'll need Go 1.20 (note: different thant the Basin CLI) and can install it with:

```sh
go install github.com/ipld/go-car/cmd/car@latest
```

## Usage

Running `src/main.py` will fetch remote files from Tableland Basin, extract the contents with `go-car`, load the resulting parquet files into a DuckDB in-memory database, run queries on the data, and then collect them into a polars DataFrame for final operations (e.g., writing to files).

To use default time ranges (the full dataset), run:

```sh
make run
```

Or, you can define a custom time range with `start` and `end` arguments (Unix epoch timestamps in milliseconds), which will be used to filter the data when _queries_ are executed. Note: the timestamp range for the `xm_data`'s `p1` publication starts on `1700438400000`.

```sh
make run start=1700438400000 end=1700783999000
```

This range does _not_ impact how Basin deals/data is fetched; _all_ publications and deals will be retrieved and extracted into the `inputs` directory. However, the `cache.json` file will store all previously extracted deals, so only new deals will be fetched on subsequent runs.

Once you run the command, it'll log information about the current status of each step in the run and the total time to complete upon finishing:

```sh
[23:20:15] INFO     Getting publications...done in 0.55s
[23:20:18] INFO     Getting deals for publications...done in 3.10s
           INFO     Number of deals found: 5
           INFO     Number of new deals: 1
           INFO     Extracting data from deals...done in 79.62s
           INFO     Creating database with parquet files...done in 0.02s
[23:21:41] INFO     Executing queries...done in 3.49s
⠙ Writing results to files...
```

> Note: The program will download all files locally before creating the database and running queries, which will use up a bit of memory. For example, five wxm parquet files will total to ~1.2 GiB in terms of raw file size (each is 200-250 MiB). Over time, this will increase daily as more data is pushed to Basin.

### Flags

The following flags are available for the `main.py` script:

- `--start`: Start timestamp (in Unix ms) for the query (e.g., 1700438400000). Defaults to full range.
- `--end`: End timestamp (in Unix ms) for the query (e.g., 1700783999000). Defaults to full range.
- `--verbose`: Enable verbose logging to show stack traces for errors. Defaults to true.

### Makefile Reference

The following defines all commands available in the Makefile:

- `make install`: Install dependencies with `pip`, upgrading pip first.
- `make basin`: Install the Basin CLI from the latest release.
- `make car`: Install `go-car` from the latest release.
- `make run`: Run the `main.py` program to fetch Basin/wxm data, run queries, and write metrics to summary files.
- `make venv`: Create a virtual environment (only for local development).
- `make freeze`: Freeze dependencies (only for local development if you make changes to the deps).

## Contributing

PRs accepted.

Small note: If editing the README, please conform to the standard-readme specification.

## License

MIT AND Apache-2.0, © 2021-2023 Tableland Network Contributors
