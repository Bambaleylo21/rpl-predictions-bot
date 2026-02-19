install:
	pip install -r requirements.txt

run:
	python main.py

freeze:
	pip freeze > requirements.txt