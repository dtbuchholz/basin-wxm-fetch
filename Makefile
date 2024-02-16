# Install Basin CLI
basin:
	go install github.com/tablelandnetwork/basin-cli/cmd/vaults@latest

# Install go-car CLI
car:
	go install github.com/ipld/go-car/cmd/car@latest

install:
	pipenv install --dev

setup:
	pipenv shell
	pipenv run pre-commit install -t pre-commit
	pipenv run pre-commit install -t pre-push

format:
	pipenv run isort --diff .
	pipenv run black --check .
	pipenv run flake8
	pipenv run mypy

test:
	pipenv run pytest

coverage:
	pipenv run pytest --cov --cov-fail-under=100
	
# Run the program to fetch & query remote parquet files
run:
	@pipenv run python -m basin_wxm.__main__ \
	$(if $(start),--start $(start)) \
	$(if $(end),--end $(end)) \
	$(if $(verbose),--verbose $(verbose))