#!/bin/bash


source .env
echo $AIRFLOW_UID
echo $AWS_KEY
echo $AWS_SECRET

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

dwh_host=$(printValue CLUSTER dwh_host < /etc/applications.conf)
echo $dwh_host
echo $DWH_DB_PASSWORD
echo $DWH_DB_USER

# ./airflow info
# docker airflow-cli info


# docker exec -it cd12380-data-pipelines-with-airflow-airflow-cli bin/bash
./airflow.sh airflow users create --email student@example.com --firstname aStudent --lastname aStudent --password admin --role Admin --username admin

./airflow.sh airflow connections add 'redshift' --conn-type 'postgres' --conn-login $DWH_DB_USER --conn-password $DWH_DB_PASSWORD --conn-host $dwh_host --conn-port 5439 --conn-schema 'dev'
./airflow.sh airflow connections add 'aws_credentials' --conn-type 'aws' --conn-login $AWS_KEY --conn-password $AWS_SECRET



#https://unix.stackexchange.com/questions/444296/section-wise-variable-accessing-from-config-file