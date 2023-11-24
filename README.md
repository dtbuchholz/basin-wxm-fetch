# Tableland Basin + WeatherXM Demo

[![standard-readme compliant](https://img.shields.io/badge/standard--readme-OK-green.svg)](https://github.com/RichardLitt/standard-readme)

> Compute queries for wxm data pushed to Tableland Basin

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
  - [Makefile Reference](#makefile-reference)
- [Contributing](#contributing)
- [License](#license)

## Background

This project contains a simple setup wherein data pushed to Tableland Basin (replicated to Filecoin) is fetched remotely and queried with [polars](https://www.pola.rs/).

## Install

To set things up on your machine, you'll need to do the following:

1. Set up the virtual environment: `python -m venv env`
2. Upgrade pip: `pip install --upgrade pip`
3. Source the `env` to use it: `source env/bin/activate`

Then, you can use the Makefile command to install dependencies: `make install`

Alternatively, you can do all of these in one step (for local development) with: `make setup`

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

This does not impact how Basin deals/data is fetched; _all_ publications and deals will be retrieved. Note: the timestamp range for the wxm data namespace is for Oct 15-21: `1697328000000` to `1697932798895`

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
