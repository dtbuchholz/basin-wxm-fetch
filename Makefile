# Only for development
.PHONY: venv
venv:
	python -m venv env

# Only for development
.PHONY: freeze
freeze:
	pip freeze > requirements.txt

# Install Basin CLI
basin:
	go install github.com/tablelandnetwork/basin-cli/cmd/basin@latest

# Install go-car CLI
car:
	go install github.com/ipld/go-car/cmd/car@latest

# Install dependencies
install:
	pip install --upgrade pip
	pip install -r requirements.txt
	
# Run the program to fetch & query remote parquet files
run:
	@python src/main.py \
	$(if $(start),--start $(start)) \
	$(if $(end),--end $(end)) \
	$(if $(verbose),--verbose $(verbose))