import boto3
import time


ecs_client = boto3.client('ecs')


cluster_name = '<cluster-name>'
task_family = '<task-family-name>'
task_revision = '<task-revision>'
subnets = ['<subnet-1', '<subnet-2>']
security_groups = ['<sg-1>', '<sg-2>']
container_name = '<container-name>'
log_group = '<log-group-name>' 
log_stream_prefix = '<log-stream-prefix>'
task_definition_with_revision = f"{task_family}:{task_revision}"


response = ecs_client.run_task(
    cluster=cluster_name,
    launchType='EC2',
    taskDefinition=task_definition_with_revision,
    count=1,
    networkConfiguration={
        'awsvpcConfiguration': {
            'subnets': subnets,
            'securityGroups': security_groups,
            'assignPublicIp': 'DISABLED'
        }
    },
    overrides={
        'containerOverrides': [
            {
                'name': container_name,
                'environment': [
                    {'name': 'CREATE_VENDOR_STANDARD_TABLES_FILE', 'value': '<input-json-file>.json'},
                    {'name': 'HISTORICAL_AD_HOC_DATA_RUN_DATE', 'value': '<trigger-for-date>'},
                    {'name': 'HISTORICAL_PROCESSING_INDICATOR', 'value': 'True'},
                    {'name': 'HISTORICAL_RUN_DATES_CSV_FILE_PATH', 'value': ''},
                    {'name': 'HISTORICAL_RUN_DATES_CSV_FILE_S3_BUCKET', 'value': ''},
                    {'name': 'INPUT_DATA_JSON_S3_BUCKET', 'value': '<input-bucket-name>'},
                    {'name': 'INPUT_DATA_JSON_S3_PATH', 'value': '<input-file-s3-path>.json'},
                    {'name': 'MUTE_QA_CHECKS_INDICATOR', 'value': 'False'},
                    {'name': 'SPARK_DRIVER_MEMORY', 'value': ''},
                    {'name': 'SPARK_EXECUTOR_MEMORY', 'value': ''},
                    {'name': 'TABLE_DELTA_PROCESSING_INDICATOR', 'value': 'True'}
                ]
            }
        ]
    }
)

task_arn = response['tasks'][0]['taskArn']

time.sleep(5)

task_info = ecs_client.describe_tasks(
    cluster=cluster_name,
    tasks=[task_arn]
)

# Get EC2 instance ID and container instance details
container_instance_arn = task_info['tasks'][0]['containerInstanceArn']
started_at = task_info['tasks'][0].get('startedAt', 'N/A')

container_instance_info = ecs_client.describe_container_instances(
    cluster=cluster_name,
    containerInstances=[container_instance_arn]
)
ec2_instance_id = container_instance_info['containerInstances'][0]['ec2InstanceId']

# Get log stream URL for CloudWatch logs
log_stream_name = f'{log_stream_prefix}/{task_arn.split("/")[-1]}'
log_url = f'https://{ecs_client.meta.region_name}.console.aws.amazon.com/cloudwatch/home?region={ecs_client.meta.region_name}#logEventViewer:group={log_group};stream={log_stream_name}'

# Optional: Output additional information for further checks
print(f'Task ARN: {task_arn}')
print(f'Task is running on EC2 instance: {ec2_instance_id}')
print(f'Container Instance ARN: {container_instance_arn}')
print(f'Task Started At: {started_at}')
print(f'CloudWatch Log URL: {log_url}')