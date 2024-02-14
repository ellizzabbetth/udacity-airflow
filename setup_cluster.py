
import argparse
import configparser
import json
import boto3  
from botocore.exceptions import ClientError
from time import sleep

import psycopg2
import sys
import csv



def get_config():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    return config



# BOM is omitted in the result
def update_credentials_aws(path_to_file: str = "utils/awsuser_accessKeys.csv"):

    result = {'AWS_KEY': 'Access key ID', 'AWS_SECRET': 'Secret access key'}
    with open(path_to_file, 'r',  encoding='utf-8-sig') as csvfile:
     
        reader = csv.DictReader(csvfile)
        dict_item = list(map(dict, reader))
        result = dict((key, dict_item[0][result[key]]) for key in result.keys())
    update_configfile(result, 'AWS') 

def update_configfile(items_info: dict, section: str):
    config = get_config()
    aws_config = config[section]
    
    for key, value in items_info.items():
        aws_config[key] = value

    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)

def create_client(DWH_REGION, AWS_KEY, AWS_SECRET):


    print("\nCreating ec2, s3, iam, redshift ...")
    ec2 = boto3.resource('ec2',
                         region_name=DWH_REGION,
                         aws_access_key_id=AWS_KEY,
                         aws_secret_access_key=AWS_SECRET
                         )

    s3 = boto3.resource('s3',
                        region_name=DWH_REGION,
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET
                        )

    iam = boto3.client("iam",
                       region_name=DWH_REGION,
                       aws_access_key_id=AWS_KEY,
                       aws_secret_access_key=AWS_SECRET
                       )

    redshift = boto3.client("redshift",
                            region_name=DWH_REGION,
                            aws_access_key_id=AWS_KEY,
                            aws_secret_access_key=AWS_SECRET
                            )

    return ec2, iam, redshift


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    creates IAM role for Redshift access to S3.
    :param iam: client object for IAM
    :param DWH_IAM_ROLE_NAME: string name for role
    :return: role_arn object (IAM_ROLE_ARN in dwh.cfg)
    """

    print("\n Creating iam role...")
    try:
        iam_role = iam.create_role(Path='/',
                               RoleName=DWH_IAM_ROLE_NAME,
                               Description="Allows Redshift to access S3 (read only)",
                               AssumeRolePolicyDocument=json.dumps(
                                   {
                                       'Statement':
                                           [
                                               {
                                                   'Action': 'sts:AssumeRole',
                                                   'Effect': 'Allow',
                                                   'Principal':
                                                       {
                                                           'Service': 'redshift.amazonaws.com'
                                                       }
                                               }
                                           ],
                                       'Version': '2012-10-17'
                                   }
                               )
                               )
    except Exception as e:
        print("\n\n Exception creating iam_role: {}".format(e))

    # attach policy to iam_role (S3 read only access)
    print("\n\n Attaching policy...")
    try:
        response = iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )['ResponseMetadata']['HTTPStatusCode']

        if response != 200:
            print("\n\n Error requesting policy, output: {}".format(str(response)))
            sys.exit(1)

        role_arn = iam.get_role(
            RoleName=DWH_IAM_ROLE_NAME
        )['Role']['Arn']

        print("\n\n Role_arn: {}".format(str(role_arn)))

        return role_arn

    except Exception as e:
        print("\n Exception attaching policy: {}".format(e))


def create_redshift_cluster(redshift,
                            DWH_CLUSTER_IDENTIFIER, DWH_NODE_TYPE, DWH_NUM_NODES,
                            DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT,
                            IAM_ROLE_ARN):
    """
    launchs Redshift cluster with given config parameters (cfg file).
    :param redshift: redshift client
    :param DWH_CLUSTER_IDENTIFIER: config parameter
    :param DWH_NODE_TYPE: config parameter
    :param DWH_NODE_TYPE: config parameter
    :param DWH_NUM_NODES: config parameter
    :param DWH_DB_NAME: config parameter
    :param DWH_DB_USER: config parameter
    :param DWH_DB_PASSWORD: config parameter
    :param DWH_PORT: config parameter
    :param IAM_ROLE_ARN: role_arn object (from func create_iam_role)
    :return: cluster info/section
    """

    print("\n\n Creating cluster...")
    try:
        response = redshift.create_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,

            ClusterType=DWH_NODE_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            DBName=DWH_DB_NAME,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            Port=int(DWH_PORT),

            IamRoles=[IAM_ROLE_ARN]
            )

    except ClientError as err:
        print("\n Exception creating cluster. Error : {}".format(err))
        return None

    else:
        return response['Cluster']


def get_cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    checks cluster status & gets info (important: HOST & ARN).
    :param redshift:
    :param DWH_CLUSTER_IDENTIFIER:
    :return: cluster_info, DWH_ENDPOINT, IAM_ROLE_ARN
    """

    while True:
        response = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)

        cluster_info = response['Clusters'][0]
        print(cluster_info['ClusterStatus'])
        if cluster_info['ClusterStatus'] == 'paused':
            print("\n{} is paused.".format(DWH_CLUSTER_IDENTIFIER))
            response = redshift.resume_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
            

        if cluster_info['ClusterStatus'] == 'available':
            print("\n{} is available.".format(DWH_CLUSTER_IDENTIFIER))
            break


        print("\nsetting up {}, wait...".format(DWH_CLUSTER_IDENTIFIER))
        sleep(60)

    try:
        DWH_ENDPOINT = cluster_info['Endpoint']['Address']
        IAM_ROLE_ARN = cluster_info['IamRoles'][0]['IamRoleArn']
        print("\nDWH_ENDPOINT: {}".format(DWH_ENDPOINT))
        print("\nDWH_ROLE_ARN: {}".format(IAM_ROLE_ARN))

        return cluster_info, DWH_ENDPOINT, IAM_ROLE_ARN

    except Exception as err:
        print("\nexception getting host & arn, error: {}".format(err))


def open_tcp_port(ec2, cluster_info, DWH_PORT):
    """
    opens incoming tcp port on EC2.
    :param ec2: aws resource
    :param cluster_info: cluster parameters
    :param DWH_PORT: EC2 port
    :return: defaultSg (EC2 default security group) (IAM_SG dwh.cfg)
    """

    print("\n\n Opening tcp port...")
    try:
        vpc = ec2.Vpc(id=cluster_info['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print("\nSG: {}".format(str(defaultSg)))
        print("\nSG ID: {}".format(defaultSg.id))
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )

    except ClientError as err:
        if 'ALLOW" already exists' in err.response['Error']['Message']:
            print("\n security group ok")

        else:
            print("\n Exception configuring security group, error: {}".format(err))

    return defaultSg.id


def check_cluster_conn(DWH_ENDPOINT, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_DB_NAME):

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        DWH_ENDPOINT, DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    print("\nconnection to redshift database is validated.")
    conn.close()


def connect_database():
    """connect database

    connect database connects to the redshift database
    Returns:
    conn: database connection.

    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    HOST                = config.get('CLUSTER', 'DWH_HOST')
    DB_NAME             = config.get('CLUSTER', 'DWH_DB_NAME')
    DB_USER             = config.get('CLUSTER', 'DWH_DB_USER')
    DB_PASSWORD         = config.get('CLUSTER', 'DWH_DB_PASSWORD')
    DB_PORT             = config.get('CLUSTER', 'DWH_PORT')
    CONNECTION_STRING   = "host={} dbname={} user={} password={} port={}".format(
        HOST,
        DB_NAME, 
        DB_USER, 
        DB_PASSWORD, 
        DB_PORT,
    )
    print('Connecting to RedShift', CONNECTION_STRING)
    conn = psycopg2.connect(CONNECTION_STRING)
    print('Connected to Redshift')
    return conn


# Create table
"""
There is 1 functions that reate table that store data in PostgreSQL
    - create_tables_from_file: run sql statement to create table
"""
def create_tables_from_file(conn, cur, path_to_file):
    with open(path_to_file, 'r') as file:
        query = file.read()
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print("Could not create table", e)
        
def setup_cluster(args):
    # gets parameters from config file dwh.cfg
    config = configparser.ConfigParser()
    config.read_file(open("dwh.cfg"))

    
    AWS_KEY                = config.get("AWS", "AWS_KEY")
    AWS_SECRET             = config.get("AWS", "AWS_SECRET")


    DWH_NODE_TYPE          = config.get("CLUSTER", "DWH_NODE_TYPE")
    DWH_NUM_NODES          = config.get("CLUSTER", "DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("CLUSTER", "DWH_NODE_TYPE")
    DWH_REGION             = config.get("CLUSTER", "DWH_REGION")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("CLUSTER", "DWH_IAM_ROLE_NAME")
    DWH_DB_NAME            = config.get("CLUSTER", "DWH_DB_NAME")
    DWH_DB_USER            = config.get("CLUSTER", "DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("CLUSTER", "DWH_DB_PASSWORD")
    DWH_PORT               = config.get("CLUSTER", "DWH_PORT")

    
    if args.launch:
        update_credentials_aws()
        ec2, iam, redshift = create_client(DWH_REGION, AWS_KEY, AWS_SECRET)

        role_arn = create_iam_role(iam, DWH_IAM_ROLE_NAME)


        cluster_info = create_redshift_cluster(redshift,
                                DWH_CLUSTER_IDENTIFIER, DWH_NODE_TYPE, DWH_NUM_NODES,
                                DWH_DB_NAME, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT,
                                role_arn)


        cluster_info, DWH_ENDPOINT, IAM_ROLE_ARN = get_cluster(redshift, DWH_CLUSTER_IDENTIFIER)


        IAM_SG = open_tcp_port(ec2, cluster_info, DWH_PORT)


        # update values in configuration file
        config.set("CLUSTER", "DWH_HOST", str(DWH_ENDPOINT))
        config.set("IAM_ROLE", "IAM_ROLE_ARN", str(IAM_ROLE_ARN))
        config.set("IAM_ROLE", "IAM_SG", str(IAM_SG))

        with open("dwh.cfg", 'w') as configfile:
            config.write(configfile)
        print("\n Values of DWH_HOST, IAM_ROLE_ARN & IAM_SG updated in configuration file.")

        check_cluster_conn(DWH_ENDPOINT, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_DB_NAME)
        
    if args.create_table:
        conn = connect_database()
        cur = conn.cursor()
        path_to_file = 'create_tables.sql'
        print("Creating table...")
        create_tables_from_file(conn, cur, path_to_file)
        print("Tables created successfully")
        conn.close()
    
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="An action working with cluster: launch or create_table")
    parser.add_argument('--launch', dest='launch', default=False, action='store_true', help="Launch Redshift cluster")
   
    parser.add_argument('--create_table', dest='create_table',default=False, action='store_true', help='Create and load data into tables')
    args = parser.parse_args()
    setup_cluster(args=args)
