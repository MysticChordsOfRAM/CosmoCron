import sys
import os
import subprocess
import time
import psycopg2
from datetime import datetime
import supersecrets as shh

def logging(job_name, command, start_dt, end_dt, duration, exit_code, output):

    status = "SUCCESS" if exit_code == 0 else "FAILURE"

    device_name = 'COSMO'

    home = psycopg2.connect({"host": shh.db_ip,
                             "database": shh.db_name,
                             "user": shh.db_user,
                             "password": shh.db_password,
                             "port": shh.db_port})

    sql = """
    INSERT INTO job_history 
    (job_name, device_name, command, start_time, end_time, duration_seconds, exit_code, status, output_log)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with home.cursor() as cur:
        cur.execute(sql, (job_name, device_name, command, start_dt, end_dt, duration, exit_code, status, output))
        home.commite()
    home.close()

def main():
    if len(sys.argv) < 3:
        print("You broke it")
        sys.exit(1)
    
    job_name = sys.argv[1]
    command_list = sys.argv[2:]
    command_str = ' '.join(command_list)

    start_time = time.time()
    start_dt = datetime.now()

    result = subprocess.run(
        command_list,
        capture_output = True,
        text = True,
        check = False,
        cwd = '/app'
    )
    exit_code = result.returncode
    output_log = f"STDOUT:\n{result.stdout}\n\nSTDERR:\,{result.stderr}"

    end_time = time.time()
    end_dt = datetime.now()
    duration = end_time - start_time

    logging(
        job_name,
        command_str,
        start_dt,
        end_dt, 
        duration,
        exit_code,
        output_log
    )

    sys.exit(exit_code)