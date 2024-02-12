
clean:
	rm -f venv

venv:
	python -m venv .venv
	pip install -r requirements.txt
	.venv\Scripts\activate

activate:
	.venv\Scripts\activate

install:
	export AIRFLOW_HOME=~/airflow
	setup_cluster.py

connections:
	source ./connections.sh




