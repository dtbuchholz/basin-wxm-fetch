# Basin + WeatherXM Queries Demo

[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg)](https://github.com/RichardLitt/standard-readme)

> Compute queries for WeatherXM data pushed to Textile Basin + Vaults

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

The script fetches data from the WeatherXM's `wxm.weather_data_dev` vault (created by `0x75c9eCb5309d068f72f545F3fbc7cCD246016fE0`) on a cron schedule with GitHub Actions. For every run, it will query data and write the results to:

-   [Data](./Data.md): Summary metrics for the run, including averages across all columns.
-   [History](./history.csv): A CSV file containing the full history of all runs, along with the run date and time.

## Install

To set things up (for local development), you'll need to do the following. First, install `pipx` and `pipenv`:

```
python3 -m pip install pipx
python3 -m pipx ensurepath
pipx install pipenv
pipenv run pip install --upgrade pip setuptools wheel
```

Then, install dependencies:

```sh
pipenv install --dev
```

And then, activate the virtual environment and set up pre-commit hooks:

```sh
pipenv shell
pipenv run pre-commit install -t pre-commit
pipenv run pre-commit install -t pre-push
```

Note the core dependencies installed are:

-   [`contextily`](https://contextily.readthedocs.io/en/latest/): Used for plotting data on a map.
-   [`duckdb`](https://duckdb.org/docs/api/python/overview): Creates an in-memory SQL database for querying parquet files extracted from Basin.
-   [`geopandas`](https://geopandas.org/en/stable/): Also used for plotting data on a map.
-   [`shapely`](https://pypi.org/project/shapely/): Required for `geopandas` to work.
-   [`requests`](https://pypi.org/project/requests/): Make requests to Basin HTTP API to fetch data.
-   [`polars`](https://pola.rs/): Used for DataFrame operations as part of post-query logic.
-   [`pyarrow`](https://pypi.org/project/pyarrow/): Required for DuckDB to work with parquet files.
-   [`rich`](https://github.com/Textualize/rich): Used for logging purposes.

Once you've done this, you'll also need to make sure the [Basin CLI](https://github.com/tablelandnetwork/basin-cli) is installed; it's part of the underlying application logic. You'll need [Go](https://go.dev/doc/install) 1.21 installed to do this, and then run:

```sh
go install github.com/tablelandnetwork/basin-cli/cmd/vaults@latest
```

Also, the [`go-car`](https://github.com/ipld/go-car) CLI is required to extract the underlying parquet files from the CAR files retrieved from Basin. You'll need Go 1.20 (note: different thant the Basin CLI) and can install it with:

```sh
go install github.com/ipld/go-car/cmd/car@latest
```

## Usage

Running `basin_wxm/main.py` will fetch remote files from Tableland Basin, extract the contents with `go-car`, load the resulting parquet files into a DuckDB in-memory database, run queries on the data, and then collect them into a polars DataFrame for final operations (e.g., writing to files).

To use default time ranges (the full dataset), run:

```sh
make run
```

Or, you can define a custom time range with `start` and `end` arguments (Unix epoch timestamps), which will be used to filter the data when _queries_ are executed. Note: the timestamp range for the `wxm.weather_data_dev` vault starts on `1707177600`.

```sh
make run start=1707177600 end=1707955200
```

This range does _not_ define which events/data is fetched; the `cache.json` file will store all previously extracted events, so only new events will be fetched on subsequent runs.

Once you run the command, it'll log information about the current status of each step in the run and the total time to complete upon finishing:

```sh
[23:20:15] INFO     Getting vaults...done in 0.55s
[23:20:18] INFO     Getting events for vaults...done in 3.10s
           INFO     Number of events found: 5
           INFO     Number of new events: 1
           INFO     Extracting data from events...done in 79.62s
           INFO     Creating database with parquet files...done in 0.02s
[23:21:41] INFO     Executing queries...done in 3.49s
⠙ Writing results to files...
```

> Note: The program will download all files locally before creating the database and running queries, which will use up a bit of memory. For example, five wxm parquet files will total to ~1.2 GiB in terms of raw file size (each is 200-250 MiB). Over time, this will increase daily as more data is pushed to Basin.

### Flags

The following flags are available for the `main.py` script:

-   `--start`: Start timestamp (in Unix ms) for the query (e.g., 1700438400000). Defaults to full range.
-   `--end`: End timestamp (in Unix ms) for the query (e.g., 1700783999000). Defaults to full range.
-   `--verbose`: Enable verbose logging to show stack traces for errors. Defaults to true.

### Makefile Reference

The following defines all commands available in the Makefile:

-   `make run`: Run the `main.py` program to fetch Basin/wxm data, run queries, and write metrics to summary files.
-   `make install`: Install dependencies with `pip`, upgrading pip first.
-   `make setup`: Use the virtual environment and set up pre-commit hooks.
-   `make format`: Run `black`, `isort`, `mypy`, and `flake8` to format and lint the code.
-   `make basin`: Install the Basin CLI from the latest release.
-   `make car`: Install `go-car` from the latest release.
-   `make test`: Run the (dummy) tests.

## Contributing

PRs accepted.

Small note: If editing the README, please conform to the standard-readme specification.

## License

MIT AND Apache-2.0, © 2021-2023 Textile Contributors
