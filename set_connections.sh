#!/bin/bash
set -o allexport
source .env
set +o allexport

echo $dwh_host
echo $AIRFLOW_UID
echo $AWS_KEY
echo $AWS_SECRET
echo $DWH_DB_USER

# Define the section and variable names
section="CLUSTER"
variable="TEST"

# Get the value of the variable from the .env file
[ ! -f .env ] || export $(grep -v '^#' .env | xargs)
value=$(grep "^${section}\[" .env | cut -d'[' -f2 | cut -d']' -f1 | grep "^${variable}=" | cut -d'=' -f2)

# Print the value
echo "The value of ${variable} in ${section} is: ${value}"
printValue()
{
  section="$1"
  param="$2"
  found=false
  while read line
  do
    [[ $found == false && "$line" != "[$section]" ]] &&  continue
    [[ $found == true && "${line:0:1}" = '[' ]] && break
    found=true
    [[ "${line%=*}" == "$param" ]] && { echo "${line#*=}"; break; }
  done
}

# get variable from section namt
dwh_host=$(printValue CLUSTER dwh_host)
echo $dwh_host
DWH_DB_PASSWORD=$(printValue CLUSTER DWH_DB_PASSWORD < /etc/applications.conf)
echo $DWH_DB_PASSWORD


# ./airflow info
# docker airflow-cli info


# docker exec -it cd12380-data-pipelines-with-airflow-airflow-cli bin/bash
./airflow.sh airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin

./airflow.sh airflow connections add 'redshift' --conn-type 'postgres' --conn-login $DWH_DB_USER --conn-password $DWH_DB_PASSWORD --conn-host $dwh_host --conn-port 5439 --conn-schema 'dev'
./airflow.sh airflow connections add 'aws_credentials' --conn-type 'aws' --conn-login $AWS_KEY --conn-password $AWS_SECRET



#https://unix.stackexchange.com/questions/444296/section-wise-variable-accessing-from-config-file