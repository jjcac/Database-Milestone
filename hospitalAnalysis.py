# 12/3/2025
# John Caceres, Mark Pfister
# Team 14
# 
# A Python script to extract data for visualization for Milestone 3.
#

import pandas as pd
import pymysql
# We switched to using pyplot instead of express, out of familiarity.
import matplotlib.pyplot as plt
import warnings
from datetime import datetime

# A warning pops up when using pd.read_sql()
warnings.filterwarnings('ignore')

# -----------------------------
# Connect to AWS
# -----------------------------
def make_connection():
    return pymysql.connect(
            host="db325-instance.cxua2a04s2fl.us-east-2.rds.amazonaws.com",  # RDS endpoint = host
            port=3306,                                       # MySQL default port
            user="admin",
            password="YsRxN9joFUsiqxkCVs1i",
            database="hospital_db"                          # or None if you CREATE first
        )

# -----------------------------
# Fetch data from MySQL
# -----------------------------
conn = make_connection()
queries = ("SELECT SUM(patients_request) AS total_patients_request, SUM(patients_admitted) AS total_patients_admitted, service FROM services_weekly GROUP BY service;",
           "SELECT service, satisfaction FROM patient;",
           "SELECT role FROM staff;",
           "SELECT month, patients_request FROM services_weekly;",
           "SELECT staff_name, present FROM staff_schedule;")

dfs = []
for query in queries: 
    dfs.append(pd.read_sql(query, conn))
conn.close()

# -----------------------------
# Plotly Express Charts
# -----------------------------

selection = (int) (input("Which graph would you like to see?\n"))

while (selection != -1):
    if (selection == 1):
        # 1. Bar Chart: Patient Requests vs Admissions by Service
        plt.figure(figsize=(12, 8))
        x = range(len(dfs[0].index))
        width = 0.35

        plt.bar(x, dfs[0]['total_patients_request'], width, label='Patient Requests')
        plt.bar([i + width for i in x], dfs[0]['total_patients_admitted'], width, label='Patients Admitted')

        plt.xlabel('Service')
        plt.ylabel('Number of Patients')
        plt.title('Patient Requests vs Admissions by Service')
        plt.xticks([i + width/2 for i in x], dfs[0].index, rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.show()

        selection = (int) (input("Which graph would you like to see?\n"))
    elif (selection == 2):
        avg_satisfaction_by_service = dfs[1].groupby('service')['satisfaction'].mean()
        best_service = avg_satisfaction_by_service.idxmax()
        best_value = avg_satisfaction_by_service.max()
        worst_service = avg_satisfaction_by_service.idxmin()
        worst_value = avg_satisfaction_by_service.min()

        colors1 = []
        for service in avg_satisfaction_by_service.index:
            if service == best_service:
                colors1.append("green")   
            elif service == worst_service:
                colors1.append("red")     
            else:
                colors1.append("lightblue") 
                
        plt.figure(figsize=(10,8))
        plt.bar(avg_satisfaction_by_service.index, avg_satisfaction_by_service.values, color=colors1)
        plt.title("Average Satisfaction by Service")
        plt.xlabel("Service")
        plt.ylabel("Average Satisfaction")
        plt.xticks(rotation=45)
        plt.show()

        selection = (int) (input("Which graph would you like to see?\n"))

    elif(selection == 3):
        role_distribution = dfs[2]['role'].value_counts()

        plt.figure(figsize=(10,8))
        colors3 =  ["#8FD694","#9AD0EC","#F7DB6A"]

        plt.pie(role_distribution,
        labels=role_distribution.index,
        autopct='%1.1f%%',         
        startangle=90,
        colors=colors3)

        plt.title("Staff Distribution by Role")
        plt.show()

        selection = (int) (input("Which graph would you like to see?\n"))
    
    elif(selection == 4):
        month_distribution = dfs[3].groupby('month')['patients_request'].sum()

        plt.figure(figsize=(10,8))
        plt.plot(month_distribution.index, month_distribution.values, marker='o')
        plt.title("Monthly Patient Demand Trend")
        plt.xlabel("Month")
        plt.ylabel("Total Patient Requests")

        plt.show()

        selection = (int) (input("Which graph would you like to see?\n"))
    
    elif(selection == 5):
        staff_attendance = dfs[4].groupby('staff_name')['present'].mean() * 100

        plt.figure(figsize=(10,8))
        plt.hist(staff_attendance, bins=10,edgecolor='black')

        plt.title("Distribution of Staff Attendance Rates")
        plt.xlabel("Attendance Rate (%)")
        plt.ylabel("Number of Staff")
        plt.show()

        plt.show()
        
        selection = (int) (input("Which graph would you like to see?\n"))
    else:
        selection = (int) (input("Invalid graph number. Which graph would you like to see?\n(Enter -1 to quit.)\n"))

print("Goodbye.")