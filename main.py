# api
from fastapi import FastAPI

# google cloud platform
from google.cloud.sql.connector import Connector
from google.auth import load_credentials_from_file

# sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text

# server
import uvicorn

# data manipulation
import pandas as pd

# file manipulation
import os


# credentials filepath
DB_PASSWORD = open(os.path.join('credentials', 'db-password.txt'), 'r').readline()

fp = os.path.join("credentials", "globant-challenge-422803-f244aacbe22b.json")
GOOGLE_CREDENTIALS = load_credentials_from_file(fp)[0]

# initialize Cloud SQL Python Connector
connector = Connector(
    credentials=GOOGLE_CREDENTIALS
)

def getconn():
    conn = connector.connect(
        "globant-challenge-422803:us-central1:globant-sql-db", # Cloud SQL Instance Connection Name
        "pymysql",
        user="db-user",
        password=DB_PASSWORD,
        db="globant-db",
        ip_type="public"
        )
    return conn


# create connection pool
pool = create_engine(
    "mysql+pymysql://",
    creator=getconn
)


def query_db(query: str):
    """Query Google Cloud SQL database

    Args:
        query (str): SQL query

    Returns:
        list: list 
    """

    with pool.connect() as db_conn:
        result = db_conn.execute(text(query))
        
        # format data as json
        result = result.mappings().all()
        return result


# initialize FastAPi instance
app = FastAPI()


def batch_insert(table: str, cols: list, fp: str):
    """ Upload historical data from CSV file to new DB

    Args:
        table (str): DB table name
        cols (list): Name of columns from table
        fp (str): CSV filepath
    """

    # Get data on dataframe to batch insert
    df = pd.read_csv(fp, header=None)
    df.columns = cols

    # single batch request that handles up to 1000 rows
    df.to_sql(table, con=pool, chunksize=1000, if_exists='append', method='multi', index=False)
    
    
@app.post('/migrate')
async def migrate(fp_jobs: str, fp_departments: str, fp_employees: str):
    """ Receive historical data from CSV files and inser data into new DB"""
    
    # insert jobs.csv
    jobs_cols = ['id', 'job']
    batch_insert('jobs', jobs_cols, fp_jobs)
    
    # insert departments.csv
    departments_cols = ['id', 'department']
    batch_insert('departments', departments_cols, fp_departments)
    
    # insert hired employees.csv
    employees_cols = ['id', 'name', 'datetime', 'department_id', 'job_id']
    batch_insert('hired_employees', employees_cols, fp_employees)

    return {'Data': 'sent'}


@app.get('/quarter')
def quarter():
    """ Section 2: Metric 1: End-point to retrieve number of employees hired
    for each job and department in 2021 divided by quarter. The table is
    ordered alphabetically by department and job."""
    
    query = """
    SELECT
        departments.department as department,
        jobs.job as job,
        SUM(CASE WHEN QUARTER(hired_employees.datetime) = 1 THEN 1 ELSE 0 END) AS Q1,
        SUM(CASE WHEN QUARTER(hired_employees.datetime) = 2 THEN 1 ELSE 0 END) AS Q2,
        SUM(CASE WHEN QUARTER(hired_employees.datetime) = 3 THEN 1 ELSE 0 END) AS Q3,
        SUM(CASE WHEN QUARTER(hired_employees.datetime) = 4 THEN 1 ELSE 0 END) AS Q4
    FROM
    hired_employees
    
    JOIN departments
    ON hired_employees.department_id=departments.id
    
    JOIN jobs
    ON hired_employees.job_id=jobs.id  
    
    WHERE YEAR(datetime) = "2021"
    GROUP BY departments.department, jobs.job
    ORDER BY departments.department, jobs.job
    ;
    """
    
    # Get queried table from Cloud SQL
    rows = query_db(query)
    
    # Return Data
    return {"data": rows}


@app.get('/department_hires')
def department_hires():
    """ Section 2: Metric 2: End-point to retireve list of ids, name and number of
    employees hired of each department that hired more employees than the mean of
    employees hired in 2021 for all the departments, ordered by the number of
    employees hired (descending)."""

    query = """
    SELECT
        departments.id as id,
        departments.department as department,
        COUNT(hired_employees.id) AS hired
    FROM hired_employees

    JOIN departments
    ON hired_employees.department_id=departments.id

    JOIN jobs
    ON hired_employees.job_id=jobs.id

    GROUP BY departments.id
        
    HAVING 
        COUNT(hired_employees.id) > (
            SELECT 
                AVG(hired)
            FROM 
                (SELECT 
                    departments.id,
                    COUNT(hired_employees.id) AS hired
                FROM 
                    hired_employees
                    
                JOIN departments
                ON hired_employees.department_id=departments.id

                JOIN jobs
                ON hired_employees.job_id=jobs.id
                
                WHERE YEAR(hired_employees.datetime) = "2021"
                GROUP BY departments.id
                ) AS avg_table
        )

    ORDER BY hired DESC
    ;
    """
    # Get queried table from Cloud SQL
    rows = query_db(query)
    
    # Return Data
    return {"data": rows}


if __name__ == "__main__":
    # Run server
    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
        )
