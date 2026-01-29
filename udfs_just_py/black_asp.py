@fused.udf
def udf():
    env_vars = """
    MY_ENV_VAR=123
    DB_USER=username
    DB_PASS=******
    """

    # Path to .env file in disk file system
    env_file_path = '/mnt/cache/gee_service_account.json'

    # Write the environment variables to the .env file
    with open(env_file_path, 'w') as file:
        file.write(env_vars)