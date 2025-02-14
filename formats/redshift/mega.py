import os
import time

import boto3
import requests
from sqlalchemy import text

from app import db, logger
from models import Export
from .db import Session

aws_key, aws_secret = os.getenv('AWS_ACCESS_KEY_ID'), os.getenv('AWS_SECRET_ACCESS_KEY')
s3_client = boto3.client('s3')
emr_client = boto3.client('emr', region_name='us-east-1')

exports_bucket = 'openalex-query-exports'

emr_service_role = 'EMR_DefaultRole'
emr_instance_profile = 'EMR_EC2_DefaultRole'


def export_mega_csv(export: Export) -> str:
    payload = export.args.copy()
    payload['get_rows'] = payload.get('get_rows') or payload.get('entity')
    payload.pop('entity')
    payload = {'query': payload}
    r = requests.post('https://api.openalex.org/searches', json=payload)
    r.raise_for_status()
    j = r.json()
    
    # Save the query URL to the export object
    export.query_url = f"https://staging.openalex.org/searches/{j['id']}"
    db.session.add(export)
    db.session.commit()
    logger.info(f"Added Search ID to mega export: {j['id']}")

    sql_query = j['redshift_sql'].replace("'", "''")
    s3_path = f's3://{exports_bucket}/{export.id}.csv'
    s = Session()

    # Create UNLOAD command and wrap it in text()
    unload_command = text(f"""
        UNLOAD ('{sql_query}')
        TO '{s3_path}'
        CREDENTIALS 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}'
        DELIMITER ','
        HEADER
        ADDQUOTES
        ALLOWOVERWRITE
        PARALLEL OFF;
        """)

    s.execute(unload_command)
    s.commit()
    source_prefix = export.id
    export_parts = [obj for obj in s3_client.list_objects(Bucket=exports_bucket, Prefix=source_prefix)['Contents']]
    if len(export_parts) == 1:
        return s3_path

    zip_dest_key = export.id + '.zip'
    job_id = create_zip_job(source_prefix, zip_dest_key)
    logger.info(f'Created EMR zip job {job_id} for {zip_dest_key}')
    
    wait_for_job_completion(job_id)
    for obj in export_parts:
        s3_client.delete_object(Bucket=exports_bucket, Key=obj['Key'])
    return f's3://{exports_bucket}/{zip_dest_key}'


def create_zip_job(file_prefix, destination_key):
    cluster = emr_client.run_job_flow(
        Name='Zip Redshift UNLOAD Files',
        ReleaseLabel='emr-6.10.0',
        ServiceRole=emr_service_role,
        JobFlowRole=emr_instance_profile,
        LogUri=f's3://{exports_bucket}/emr-logs/',
        Tags=[{
            'Key': 'for-use-with-amazon-emr-managed-policies',
            'Value': 'true'
        }],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.4xlarge',
                    'InstanceCount': 1,
                    'EbsConfiguration': {
                        'EbsBlockDeviceConfigs': [
                            {
                                'VolumeSpecification': {
                                    'VolumeType': 'gp2',
                                    'SizeInGB': 300
                                },
                                'VolumesPerInstance': 1
                            }
                        ]
                    }
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        Steps=[
            {
                'Name': 'Zip Files',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'bash', '-c',
                        f'''
                        set -e  # Exit on any error
                        echo "Creating directory..."
                        mkdir -p /tmp/unload_data
                        cd /tmp/unload_data

                        echo "Copying files from S3..."
                        # List all files with the prefix and download each one
                        aws s3api list-objects-v2 \
                            --bucket {exports_bucket} \
                            --prefix {file_prefix} \
                            --query 'Contents[].Key' \
                            --output text | tr '\t' '\n' | while read -r key; do
                            echo "Downloading $key..."
                            aws s3 cp "s3://{exports_bucket}/$key" .
                        done

                        echo "Checking copied files..."
                        ls -la

                        echo "Creating zip file..."
                        zip -r ../output.zip ./*

                        echo "Checking zip file..."
                        ls -la ../output.zip

                        echo "Copying zip back to S3..."
                        aws s3 cp ../output.zip s3://{exports_bucket}/{destination_key}
                        '''
                    ]
                }
            }
        ],
        VisibleToAllUsers=True,
        Applications=[{'Name': 'Hadoop'}]
    )

    return cluster['JobFlowId']


def wait_for_job_completion(job_id):
    while True:
        status = emr_client.describe_cluster(ClusterId=job_id)['Cluster']['Status']
        print(f'EMR Zip Job ({job_id}) status - {status}')
        state = status['State']

        if state in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
            break
        elif state == 'WAITING':
            steps = emr_client.list_steps(ClusterId=job_id)['Steps']
            if all(step['Status']['State'] in ['COMPLETED'] for step in steps):
                break

        time.sleep(30)

    return state == 'TERMINATED'