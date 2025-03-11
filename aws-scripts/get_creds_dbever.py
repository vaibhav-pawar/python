import subprocess
import json


git_bash_path = r"C:\Program Files\Git\bin\bash.exe"


get_tokens_command = 'aws sts assume-role --role-arn "<role-arn>" --role-session-name <session-name>'
get_tokens_execute = subprocess.run([git_bash_path, '-c', get_tokens_command], capture_output=True, text=True)
if get_tokens_execute.returncode == 0:
    print("Token details have been collected.")
    get_tokens_response = json.loads(get_tokens_execute.stdout)
    credentials = get_tokens_response['Credentials']
    access_key_id = credentials['AccessKeyId']
    secret_access_key = credentials['SecretAccessKey']
    session_token = credentials['SessionToken']

    bash_commands = f'''
    export AWS_ACCESS_KEY_ID={access_key_id} &&
    export AWS_SECRET_ACCESS_KEY={secret_access_key} &&
    export AWS_SESSION_TOKEN={session_token} &&
    aws redshift-serverless get-credentials --workgroup-name <workgroup-name> --db-name <database-name>
    '''

    set_env_and_get_creds_execute = subprocess.run([git_bash_path, '-c', bash_commands], capture_output=True, text=True)
    if set_env_and_get_creds_execute.returncode == 0:
        print("Temporary credentials collected successfully:")
        try:
            get_temp_creds_response = json.loads(set_env_and_get_creds_execute.stdout)
            print(json.dumps(get_temp_creds_response, indent=2))
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON output: {e}")
    else:
        print("Error executing get-credentials command:\n", set_env_and_get_creds_execute.stderr)
else:
    print("Error executing assume-role command:\n", get_tokens_execute.stderr)