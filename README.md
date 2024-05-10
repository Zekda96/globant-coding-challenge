![Python](https://img.shields.io/badge/Python-3670A0?style=flat-square&logo=python&labelColor=black&logoColor=ffdd54)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4.svg?style=flat-square&logo=google-cloud&labelColor=black&logoColor=4285F4)
![MySQL](https://img.shields.io/badge/MySQL-4479A1?style=flat-square&logo=sqlite&labelColor=black&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=flat-square&logo=fastapi)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=flat-square&logo=pandas&logoColor=white)

# Data Engineering Challenge

## Stack
- Python

    - FastAPI
        - POST Method (upload CSV Files to DB)
        - GET Method (key metrics endpoints)
    - Pandas
    - Google Cloud SQL API
    - SQLAlchemy
    - Uvicorn


## Features
- Google Cloud SQL Database
- FastAPI-based API
- Batch insert of up to 1000 rows from CSV files to DB
- End-points for each key metric

## Setup

1. Clone the repository
```python
git clone https://github.com/Zekda96/globant-coding-challenge.git
```

2. Install requirements on a venv
```python
$ pip install -r requirements.txt
```

3. Create a folder named `credentials` in the project directory and place the credentials files there.

4. Create a folder named `data` in the project directory and place all three .csv files there:
    - jobs.csv
    - departments.csv
    - hired_employees.csv

5. Run `main.py` to host the API server.

6. Run `post_request.py` to upload the data from the CSV files to the Google Cloud SQL database.

7. Execute the GET requests from any browser using the following end-points:

    - http://127.0.0.1:8000/quarter
    -  http://127.0.0.1:8000/department_hires



## Design 

**The main goal of this project is to safely migrate the data on the .csv files to a new database in order to allow the retrieval of key metrics.**

**API**:
A FastAPI framework is chosen over Flask or Django due to its ease of deployment and sufficient features to meet the needs of this project.

**Batch Inserts**:
In order to efficiently insert the data into the SQL database, up to 1000 rows can be handled by API. This assures minimal latency, as connections are not created in a 'per row' basis, while making an efficient use of the Cloud DB memory resources.

**SQL Database**:
Google Cloud SQL is chosen as cloud database due to its cost-efficiency for the scale of this project. Allocation of resources (memory, storage size, latency) is allowed to be minimal yet efective to handle the size of the database and processing of the SQL queries.

**SQL Queries**:
Minimal use of resources is also a consideration for the SQL queries design i.e. temporal tables were avoided to minimize memory impact.

## Production and scalability
In order to properly deploy an API, it should be hosted on a cloud server or, at least, not on the user-end. This allows the API to fulfill its purpose which is to prohibit the user **from having access to the database resources**.

Due to full compatibility with the SQL Database, **Google Cloud Run** would be ideal to deploy a containerized app to host the API.



