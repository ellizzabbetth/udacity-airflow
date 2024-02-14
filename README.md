# Data Pipelines with Airflow

Welcome to the Data Pipelines with Airflow project! This endeavor will provide you with a solid understanding of Apache Airflow's core concepts. Your task involves creating custom operators to execute essential functions like staging data, populating a data warehouse, and validating data through the pipeline.

To begin, we've equipped you with a project template that streamlines imports and includes four unimplemented operators. These operators need your attention to turn them into functional components of a data pipeline. The template also outlines tasks that must be interconnected for a coherent and logical data flow.

A helper class containing all necessary SQL transformations is at your disposal. While you won't have to write the ETL processes, your responsibility lies in executing them using your custom operators.

## Initiating the Airflow Web Server
Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop/) is installed before proceeding.

To bring up the entire app stack up, we use [docker-compose](https://docs.docker.com/engine/reference/commandline/compose_up/) as shown below

```bash
docker-compose up -d
```


### Generate AWS Credentials and store .csv in root directory

Follow these steps to create your AWS Credentials:

Log into the AWS Console
Search for and click on IAM
Create a User
Click "Add User"
Name the user uniquely within your list of IAM users
Click "Access Key - Programmatic Access"
Click "Permissions"
Click "Attach Existing Policies Directly"
Search for and click on the following:
"administratoraccess," which gives Airflow admin access to AWS
"amazonredshiftfullaccess," which gives Airflow full access to Amazon Redshift
"amazons3fullaccess," which gives Airflow full access to your S3 storage
Click "Next"
Do not add tags
Click "Review"
Click "Create User"
Click "Download.csv." This downloads your automatically generated password.'
In this document should be your Access Key Id (username) and Secret Key (password)
Keep these so you can configure the user in Airflow

### Execute shell script
```
set_connections.sh
```


### Set up aws resources
```
python setup_cluster.py --launch
```

### Create tables
```
python setup_cluster.py --create_table
```

### Inspect DAGs in ```http://localhost:8080/```

### To debug container
https://stackoverflow.com/questions/37195222/how-to-view-log-output-using-docker-compose-run
```
docker logs <containerid>

```
For example:
```
docker-compose logs airflow-webserver
```

Visit http://localhost:8080 once all containers are up and running.

## Configuring Connections in the Airflow Web Server UI
![Airflow Web Server UI. Credentials: `airflow`/`airflow`.](assets/login.png)

On the Airflow web server UI, use `airflow` for both username and password.
* Post-login, navigate to **Admin > Connections** to add required connections - specifically, `aws_credentials` and `redshift`.
* Don't forget to start your Redshift cluster via the AWS console.
* After completing these steps, run your DAG to ensure all tasks are successfully executed.

## Getting Started with the Project
1. The project template package comprises three key components:
   * The **DAG template** includes imports and task templates but lacks task dependencies.
   * The **operators** folder with operator templates.
   * A **helper class** for SQL transformations.

1. With these template files, you should see the new DAG in the Airflow UI, with a graph view resembling the screenshot below:
![Project DAG in the Airflow UI](assets/final_project_dag_graph1.png)
You should be able to execute the DAG successfully, but if you check the logs, you will see only `operator not implemented` messages.

## DAG Configuration
In the DAG, add `default parameters` based on these guidelines:
* No dependencies on past runs.
* Tasks are retried three times on failure.
* Retries occur every five minutes.
* Catchup is turned off.
* No email on retry.

Additionally, configure task dependencies to match the flow depicted in the image below:
![Working DAG with correct task dependencies](assets/final_project_dag_graph2.png)

## Developing Operators
To complete the project, build four operators for staging data, transforming data, and performing data quality checks. While you can reuse code from Project 2, leverage Airflow's built-in functionalities like connections and hooks whenever possible to let Airflow handle the heavy lifting.

### Stage Operator
Load any JSON-formatted files from S3 to Amazon Redshift using the stage operator. The operator should create and run a SQL COPY statement based on provided parameters, distinguishing between JSON files. It should also support loading timestamped files from S3 based on execution time for backfills.

### Fact and Dimension Operators
Utilize the provided SQL helper class for data transformations. These operators take a SQL statement, target database, and optional target table as input. For dimension loads, implement the truncate-insert pattern, allowing for switching between insert modes. Fact tables should support append-only functionality.

### Data Quality Operator
Create the data quality operator to run checks on the data using SQL-based test cases and expected results. The operator should raise an exception and initiate task retry and eventual failure if test results don't match expectations.

## Reviewing Starter Code
Before diving into development, familiarize yourself with the following files:
- [plugins/operators/data_quality.py](plugins/operators/data_quality.py)
- [plugins/operators/load_fact.py](plugins/operators/load_fact.py)
- [plugins/operators/load_dimension.py](plugins/operators/load_dimension.py)
- [plugins/operators/stage_redshift.py](plugins/operators/stage_redshift.py)
- [plugins/helpers/sql_queries.py](plugins/helpers/sql_queries.py)
- [dags/final_project.py](dags/final_project.py)

Now you're ready to embark on this exciting journey into the world of Data Pipelines with Airflow!


## Debug


```
docker logs -t -f --tail 100 airflow-webserver
```
refresh dags
```
docker-compose exec -it airflow-webserver airflow dags reserialize
```

or print the full stacktrace for python error found in dags

```
docker-compose exec airflow airflow list_dags
```

Restart airflow scheduler
```
docker-compose restart airflow-scheduler
```


## Feedback

Requires Changes
1 specification requires changes
Great progress you've made on your Data Pipelines with Airflow project! 🎉 Your efforts in building a robust ETL pipeline are commendable, and your DAG exhibits solid functionality, ensuring smooth data orchestration in the Airflow UI. Let's delve into what went well and explore some suggestions for further refinement. Your dedication to implementing best practices and maintaining code quality is evident, making this project a significant achievement. Well done! 👏

✅ The DAG is well-organized and easily navigable in the Airflow UI.
✅ Setting up default_args, simplifying the application of common parameters across operators.
✅ The DAG is appropriately scheduled to run once per hour, meeting the specified requirements.
✅ Dynamic functionality using params allows for flexibility in running various SQL statements for quality checks.
REQUIREMENTS
:x: Using the TRUNCATE statement instead of DELETE for advantages like ignoring delete triggers and immediate commits.
SUGGESTIONS
Difference between DROP and TRUNCATE in SQL
"Using Airflow Variables for Dynamic DAGs" for a practical guide on leveraging Airflow's variable feature.
"Efficient Data Loading to Amazon Redshift" covers strategies like using the COPY command options and optimizing data distribution
Continuous improvement will elevate the project even further. Great job, and best of luck with the enhancements! 🌟

General
DAG can be browsed without issues in the Airflow UI

Awesome! DAG can be browsed without issues in the Airflow UI. :clap:

Suggestions
To make the DAG even more compact, you could try to use the SubDag operator with the dimension loads and hide the repetitive parts behind that. Depending on your set up, using a subdag operator could make your DAG cleaner.

Using SubDAGs in Airflow
Subdag Operator examples
The dag follows the data flow provided in the instructions, all the tasks have a dependency and DAG begins with a start_execution task and ends with a end_execution task.

Excellent work! The DAG’s graph view all the task have a dependency and DAG begins with a begin_execution task and ends with a stop_execution task. :thumbsup:

Dag configuration
DAG contains default_args dictionary, with the following keys:

Owner
Depends_on_past
Start_date
Retries
Retry_delay
Catchup
Good job on binding the default_args with this dag

The DAG object has default args set

Good job defining the default_args dictionary as required.

Notes

If a dictionary of default_args is passed to a DAG, it will apply them to any of its operators. This makes it easy to apply a common parameter to many operators without having to type it many times.

External Resources

Backfill and Catchup

How to prevent airflow from backfilling dag runs?

The DAG should be scheduled to run once an hour

Good work scheduling the DAG to run once an hour as required.

Staging the data
There is a task that to stages data from S3 to Redshift. (Runs a Redshift copy statement)

Nice work the stage operator. I suggest using a template field that would allow to load timestamped files from S3 based on the execution time and run backfills.
but your current implementation is also great

template_fields = ("s3_key",) 
Check the detail here https://airflow.readthedocs.io/en/latest/howto/custom-operator.html

Instead of running a static SQL statement to stage the data, the task uses params to generate the copy statement dynamically

Good job dynamically generating the copy statement using params as opposed to static SQL statements.

The operator contains logging in different steps of the execution

logging.info shows the status of staging execution. Nice work! :thumbsup:

The SQL statements are executed by using a Airflow hook

Good job connecting to the database via an Airflow hook. :clap:

Loading dimensions and facts
Dimensions are loaded with on the LoadDimension operator

There is a separate functional operator for dimensions LoadDimensionOperator.

Facts are loaded with on the LoadFact operator

There is a separate functional operator for facts LoadFactOperator as well. :clap:

Instead of running a static SQL statement to stage the data, the task uses params to generate the copy statement dynamically

Good job dynamically generating the copy statement using params as opposed to static SQL statements.

The DAG allows to switch between append-only and delete-load functionality

Great job that you have added a truncate param to allow the switch.
However, instead of deleting the table, it would be better to use the truncate.

The TRUNCATE statement can provide the following advantages over a DELETE statement:

The TRUNCATE statement can ignore delete triggers
The TRUNCATE statement can perform an immediate commit
The TRUNCATE statement can keep storage allocated for the table
Data Quality Checks
Data quality check is done with correct operator

Great work! The operator that runs a check on the fact or dimension table(s) after the data has been loaded is DataQualityOperator. The data quality operator looks awesome. It is simple but still allows you to do import checks on the data and catch the possible data quality issues as soon as possible.

The DAG either fails or retries n times

Check if the dag fails and is not passed, by raising ValueError.

ADDITIONAL RESOURCES
Troubleshooting Airflow ValueError
airflow.exceptions --- Airflow Documentation
Operator uses params to get the tests and the results, tests are not hard coded to the operator

The parameters were used to add some dynamic functionality to the operators and allow to run various SQL statements instead of hardcoded SQL statement. Well done! :star: