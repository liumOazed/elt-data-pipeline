import subprocess
import time

## Check function
def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """
    Wait for a PostgreSQL database to become available, retrying up to `max_retries` 
    times with a delay of `delay_seconds` seconds between each retry.
    """
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                print(f"Successfully connected to {host}.")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Failed to connect to {host}: {e}")
            retries += 1
            print(f"Retrying in {delay_seconds} seconds...(Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    
    print("Max retries reached. Exiting...")
    return False

## Initialization of source database
if not wait_for_postgres("source_postgres"):
    exit(1)

# Wait for destination database
if not wait_for_postgres("destination_postgres"):
    exit(1)

print("Starting ELT script...")

source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'
}

## Destination
destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'
}

# Dump command
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD=source_config['password'])

# Run pg_dump with error handling
try:
    subprocess.run(dump_command, env=subprocess_env, check=True)
except subprocess.CalledProcessError as e:
    print(f"pg_dump failed: {e}")
    print(f"Command: {e.cmd}")
    print(f"Return code: {e.returncode}")
    print(f"Output: {e.output}")
    print(f"Error message: {e.stderr}")
    exit(1)

# From source to destination
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

subprocess_env = dict(PGPASSWORD=destination_config['password'])

# Run psql with error handling
try:
    subprocess.run(load_command, env=subprocess_env, check=True)
except subprocess.CalledProcessError as e:
    print(f"psql failed: {e}")
    print(f"Command: {e.cmd}")
    print(f"Return code: {e.returncode}")
    print(f"Output: {e.output}")
    print(f"Error message: {e.stderr}")
    exit(1)

print("Ending ELT Script...")