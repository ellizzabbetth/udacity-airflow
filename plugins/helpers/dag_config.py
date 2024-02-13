class DagConfig():
    REGION = 'us-east-1'
    S3_BUCKET = 'udacity-data-pipelines-bradley'
    DATA_FORMAT_EVENT= f"JSON 's3://{S3_BUCKET}/log_json_path.json'"
    DATA_FORMAT_SONG= "FORMAT AS JSON '{}'" #"JSON 'auto'"
    S3_LOG_KEY = 'log-data'
    S3_SONG_KEY = 'song-data'
    AWS_CREDENTIALS_ID = 'aws_credentials'
    REDSHIFT_CONN_ID = 'redshift'