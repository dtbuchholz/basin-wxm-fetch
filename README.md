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

1. Run: `python -m venv env`.
2. Source: `source env/bin/activate`

Then, you can use the `Makefile` command to install dependencies: `make install`.

## Usage

Running `src/main.py` will fetch remote files from Tableland [Basin](https://github.com/tablelandnetwork/basin-cli), load them into a `polars` dataframe, and then run queries on the data.

To use default time ranges (the full dataset), run:

```sh
make run
```

Or, you can define a custom time range:

```sh
make run start=1697328000000 end=1697932798895
```

Note: the timestamp range for wxm data namespace is for Oct 15-21: `1697328000000` to `1697932798895`

### Makefile Reference

The following defines all commands available in the `Makefile`:

- `make install`: Install dependencies.
- `make run`: Run the `main.py` program.
- `make freeze`: Freeze dependencies (only needed if you make changes to the python code deps).

## Contributing

PRs accepted.

Small note: If editing the README, please conform to the standard-readme specification.

## License

MIT AND Apache-2.0, © 2021-2023 Tableland Network Contributors
