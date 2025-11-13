# 11/12/2025
# John Caceres, Mark Pfister
# Team 14
# 
# A Python script to implement the database schema, parse the input files,
# and upload the data for Milestone 2.
#

#import pymysql,json,os
import pymysql
import os
import pandas as pd


# Set this to "GCP" or "AWS" (or use an env var: DB_PLATFORM=GCP/AWS)
PLATFORM = os.getenv("DB_PLATFORM", "GCP").upper()

def getconn():
    if PLATFORM == "GCP":
        # ----- GCP ONLY -----
        # install: pip install PyMySQL cloud-sql-python-connector
        from google.cloud.sql.connector import Connector

        # NOTE: use a raw string for Windows paths to avoid backslash escapes
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"key.json"

        connector = Connector()
        return connector.connect(
            "csc-ser325-470207:us-central1:db325-instance",   # e.g. "csc-ser325:us-central1:db325-instance"
            "pymysql",
            user="root",
            password="p22a@M~^sFn%4DQs",
            db=None               # or None if you prefer to CREATE first, then USE
        )
    else:
        # ----- AWS ONLY -----
        # install: pip install PyMySQL
        return pymysql.connect(
            host="mydb.xxxxxx.us-east-1.rds.amazonaws.com",  # RDS endpoint = host
            port=3306,                                       # MySQL default port
            user="admin",
            password="your-password",
            database=None                            # or None if you CREATE first
        )
    
def setup_db(cur):
  # Set up db
    cur.execute('CREATE DATABASE IF NOT EXISTS hospital_db')
    cur.execute('USE hospital_db')

    # Set up tables    
    cur.execute('DROP TABLE IF EXISTS services_weekly;')    
    cur.execute('DROP TABLE IF EXISTS staff_schedule;')
    cur.execute('DROP TABLE IF EXISTS staff;')
    cur.execute('DROP TABLE IF EXISTS patient;')
    
    # Create staff table with primary key staff_name.
    # Each entry represents a unique staff member.
    # staff_name is used as the primary key because staff_ids are not kept
    #   consistent in the original dataset for some reason.
    cur.execute('''
        CREATE TABLE staff (
            staff_id CHAR(12) NOT NULL,
            staff_name VARCHAR(50) NOT NULL,
            role VARCHAR(50) NOT NULL,
            service VARCHAR(50) NOT NULL,
            PRIMARY KEY (staff_name)
        );
        ''')

    # Create patient table with primary key patient_id.
    cur.execute('''
        CREATE TABLE patient (
            patient_id CHAR(12) NOT NULL,
            name VARCHAR(50) NOT NULL,
            age TINYINT(3) UNSIGNED NOT NULL,
            arrival_date DATE NOT NULL,
            departure_date DATE NOT NULL,
            service VARCHAR(16) NOT NULL,
            satisfaction TINYINT(3) UNSIGNED NOT NULL,
            PRIMARY KEY (patient_id)
        );
        ''')

    # Create services_weekly table with primary key (week, service)
    cur.execute('''
        CREATE TABLE services_weekly (
            week TINYINT(2) UNSIGNED NOT NULL,
            month TINYINT(2) UNSIGNED NOT NULL,
            service VARCHAR(16) NOT NULL,
            available_beds TINYINT(2) UNSIGNED NOT NULL,
            patients_request SMALLINT(3) UNSIGNED NOT NULL,
            patients_admitted TINYINT(2) UNSIGNED NOT NULL,
            patients_refused SMALLINT(3) UNSIGNED NOT NULL,
            patient_satisfaction TINYINT(3) UNSIGNED NOT NULL,
            staff_morale TINYINT(3) UNSIGNED NOT NULL,
            event VARCHAR(10) NOT NULL,
            PRIMARY KEY (week, service)
        );
        ''')
    
    # Create staff_schedule table with primary key (week, staff_name).
    cur.execute('''
        CREATE TABLE staff_schedule (
            week TINYINT(2) UNSIGNED NOT NULL,
            staff_name VARCHAR(50) NOT NULL,
            present TINYINT(1) UNSIGNED NOT NULL,
            PRIMARY KEY (week, staff_name),
            FOREIGN KEY (staff_name) REFERENCES staff(staff_name)
        );
        ''')

def insert_data(cur):
    cur.execute('USE hospital_db')

    # Read CSVs as DataFrames
    patientData = pd.read_csv(".\\patients.csv", header = 0)
    servicesData = pd.read_csv(".\\services_weekly.csv", header = 0)
    staffData = pd.read_csv(".\\staff.csv", header = 0)
    staffScheduleData = pd.read_csv(".\\staff_schedule.csv", header = 0)

    # Insert patient data into database
    for row in patientData.itertuples():
        cur.execute('''INSERT IGNORE INTO patient  
            VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                (row.patient_id, row.name, row.age, row.arrival_date, row.departure_date, row.service, row.satisfaction) )

    # Insert staff data into database
    for row in staffData.itertuples():
        cur.execute('''INSERT IGNORE INTO staff  
            VALUES (%s, %s, %s, %s)''',
                (row.staff_id, row.staff_name, row.role, row.service) )

    # Insert services_weekly data into database
    for row in servicesData.itertuples():
        cur.execute('''INSERT IGNORE INTO services_weekly  
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (row.week, row.month, row.service, row.available_beds, row.patients_request,
                 row.patients_admitted, row.patients_refused, row.patient_satisfaction, row.staff_morale, row.event) )

    # Insert staff_schedule data into database.
    for row in staffScheduleData.itertuples():
        cur.execute('''INSERT IGNORE INTO staff_schedule
            VALUES (%s, %s, %s)''',
                (row.week, row.staff_name, row.present) )

cnx = getconn() 
cur = cnx.cursor()
print("Starting Setup...")
setup_db(cur)
print("Finished Setup.")
print("Starting Insert...")
insert_data(cur)
print("Finished Insert.")
cur.close()
cnx.commit()
cnx.close()
print("FINISHED")
