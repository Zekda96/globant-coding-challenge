import requests

url = "http://127.0.0.1:8000/migrate?fp_jobs=./data/jobs.csv&fp_departments=./data/departments.csv&fp_employees=./data/hired_employees.csv"

payload = {}
headers = {}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
