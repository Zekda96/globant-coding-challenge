# api
from fastapi import FastAPI

# google cloud sql
from google.cloud.sql.connector import Connector

# sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text

# credentials filepath
pwrd = './credentials/db-password.txt'

# initialize Cloud SQL Python Connector
connector = Connector()

def getconn():
    conn = connector.connect(
        "globant-challenge-422803:us-central1:globant-sql-db", # Cloud SQL Instance Connection Name
        "pymysql",
        user="db-user",
        password=open(pwrd, 'r').readline(),
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

@app.get('/')
def hello():
    return {"Hello": "World"}


@app.get('/quarter')
def quarter():
    """ Section 1: Metric 1"""
    
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
    """ Section 1: Metric 2"""


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
