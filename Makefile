# Only for development
.PHONY: setup
setup:
	python -m venv env
	env/bin/pip install --upgrade pip
	env/bin/pip install -r requirements.txt

# Only for development
.PHONY: freeze
freeze:
	env/bin/pip freeze > requirements.txt

# Install dependencies
install:
	pip install -r requirements.txt
	
# Run the program to fetch & query remote parquet files
run:
	@python src/main.py $(if $(start),--start $(start)) $(if $(end),--end $(end))