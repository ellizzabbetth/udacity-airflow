
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
	setup_cluster.py --launch
	setup_cluster.py --create_table

connections:
	source set_connections.sh




