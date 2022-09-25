import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime
from posixpath import defpath

# EMAIL LIST
EmailList=["nytbsemail@gmail.com"]

# BOOK LISTS
Final_Fiction_List={}
Final_NonFiction_List={}


# API CALLS
Combined_Fiction_Response = requests.get("https://api.nytimes.com/svc/books/v3/lists/current/combined-print-and-e-book-fiction.json?api-key=IXOBVbrTwLIxYhyJKEjGSDCsGU631u4H")
Combined_NonFiction_Response = requests.get("https://api.nytimes.com/svc/books/v3/lists/current/combined-print-and-e-book-nonfiction.json?api-key=IXOBVbrTwLIxYhyJKEjGSDCsGU631u4H")

# CREATE DAG / SCHEDULE
default_args = {
    'start_date': datetime(2022, 9, 25),
    'schedule_interval': '@daily',
}

dag = DAG('Email_Bestseller_List_To_Users',
description='This will collect NYTimes bestseller list and send to specified users',
default_args=default_args,
start_date=datetime(2022, 9, 25),
catchup=False
)



# FUNCTION TO GET COMBINED NONFICTION BOOK LIST FROM NYTIMES BESTSELLER
def Get_Combined_NonFiction_Book_List():
        Combined_NonFiction_Raw_Data=Combined_NonFiction_Response.text
        #print(Combined_Fiction_Raw_Data)
        parse_json=json.loads(Combined_NonFiction_Raw_Data)
        NonFiction_List=parse_json['results']['books']
        for lis in NonFiction_List:
            for key,val in lis.items():
                Non_Fiction_List=(key.title().replace("_"," "),val)
        Final_NonFiction_List=Non_Fiction_List

#FOR DEBUGGING
# Get_Combined_NonFiction_Book_List()

# FUNCTION TO GET COMBINED FICTION BOOK LIST FROM NYTIMES BESTSELLER
def Get_Combined_Fiction_Book_List():
        Combined_Fiction_Raw_Data=Combined_Fiction_Response.text
        parse_json=json.loads(Combined_Fiction_Raw_Data)
        #print(Combined_Fiction_List)
        Fiction_List=parse_json['results']['books']
        #print(List)
        for lis in Fiction_List:
           for key,val in lis.items():
                Fiction_List=(key.title().replace("_"," "),val)
        Final_Fiction_List=Fiction_List

#FOR DEBUGGING
# Get_Combined_Fiction_Book_List()



def Get_Email_list():
    return EmailList



PushEmailListToXcom = PythonOperator(
    task_id='Push_email_list_to_xcom',
    python_callable=Get_Email_list,
    do_xcom_push=True
)
    
FictionBookList = PythonOperator(
    task_id='Fiction_book_list',
    python_callable=Get_Combined_Fiction_Book_List,
    do_xcom_push=True,
    dag=dag
)
   
NonFictionBookList = PythonOperator(
    task_id='NonFiction_book_list',
    python_callable=Get_Combined_NonFiction_Book_List,
    do_xcom_push=True,
    dag=dag
)


SendMail= EmailOperator(
       task_id='Send_Email',
       to="{{ task_instance.xcom_pull(task_ids='Push_email_list_to_xcom') }}",
       subject='Test Mail',
       html_content=Final_Fiction_List,
       dag=dag
)

PushEmailListToXcom >> FictionBookList >> NonFictionBookList >> SendMail