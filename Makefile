install:
	pip install -r requirements.txt

freeze:
	pip freeze > requirements.txt

run:
	@python src/main.py $(if $(start),--start $(start)) $(if $(end),--end $(end))