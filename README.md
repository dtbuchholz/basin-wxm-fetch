# Basin + WeatherXM Queries

## Setup

Install pipx if pipenv and cookiecutter are not installed

```
python3 -m pip install pipx
python3 -m pipx ensurepath
```

Install pipenv using pipx

```
pipx install pipenv
pipenv run pip install --upgrade pip setuptools wheel
```

Then, install deps:

```sh
# Install dependencies
pipenv install --dev

# Activate the virtual environment
pipenv shell

# Setup pre-commit and pre-push hooks
pipenv run pre-commit install -t pre-commit
pipenv run pre-commit install -t pre-push
```

## Credits

This package was created with Cookiecutter and the [sourcery-ai/python-best-practices-cookiecutter](https://github.com/sourcery-ai/python-best-practices-cookiecutter) project template.
