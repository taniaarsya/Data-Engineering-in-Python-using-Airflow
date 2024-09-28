from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import glob
import sqlite3

# Define your cleaning function
def loan_data(df):
    # Mengubah tipe data kolom 'issue_d' menjadi datetime dengan format tertentu
    df['issue_d'] = pd.to_datetime(df['issue_d'], format="%d/%m/%Y", errors='coerce')
    
    # Mengubah tipe data kolom
    df['interest_rate'] = df['interest_rate'].astype(float)
    df['loan_amount'] = df['loan_amount'].astype(int)
    df['annual_inc'] = df['annual_inc'].astype(float)
    
    # Mendefinisikan kolom kategori
    cat_column = ['home_ownership', 'income_category', 'application_type', 
                  'interest_payments', 'loan_condition', 'grade', 'region']
    df[cat_column] = df[cat_column].astype('category')
    
    # Mengembalikan dataframe yang telah dibersihkan
    return df

def fetch_clean():
    database = 'home/taniaa/airflow/dags/db/loan.db'
    conn = sqlite3.connect(database)
    files = glob.glob("home/taniaa/airflow/dags/loan_2014/*.csv")

    df_list = []

    for file in files:
        # mengambil transaksi terkini dari database
        last_update = pd.read_sql_query("""
                    SELECT issue_d 
                    FROM loan
                    ORDER BY issue_d DESC
                    LIMIT 1
                    """, con = conn)
        last_timestamp = last_update['issue_d'].to_string(index=False)

        # membaca dataframe
        trx = pd.read_csv(file)
        # membersihkan dataframe
        trx_clean = loan_data(trx)

        # filtering dataframe
        trx_clean_update = trx_clean[trx_clean['issue_d'] > last_timestamp]
        
        # jika dataframe terisi, maka masukkan ke dalam df_list
        if trx_clean_update.shape[0] != 0:
            df_list.append(trx_clean_update)
    
    return df_list


def df_to_db(ti):
    database = 'home/taniaa/airflow/dags/db/loan.db'
    conn = sqlite3.connect(database)
    df_list = ti.xcom_pull(task_ids = 'fetch_clean_task')

    # Iterasi DataFrame
    for df in df_list:
        df.to_sql(name='loan',
                  con=conn,
                  if_exists='append',
                  index=False)
        print("Done Created DB")


def report_generator(ti):

    df_list = ti.xcom_pull(task_ids = 'fetch_clean_task')
    
    for df in df_list:
        periode = df['issue_d'].dt.to_period('M').unique()[0]

        # Cross-tabulation untuk menghitung frekuensi 'loan_condition' dan 'region'
        cross_tab_count = pd.crosstab(index=df['region'],
                                      columns=df['loan_condition'])
        
        # Cross-tabulation untuk menjumlahkan 'loan_amount' berdasarkan 'loan_condition' dan 'region'
        cross_tab_sum = pd.crosstab(index=df['region'],
                                    columns=df['loan_condition'],
                                    values=df['loan_amount'],
                                    aggfunc='sum')
        
    #     # Menggabungkan kedua tabel
    #     cross_tab_combined = pd.concat([cross_tab_count, cross_tab_sum], axis=1, keys=['Count', 'Total Loan Amount'])
        
    #     # Melakukan pivot untuk mendapatkan format yang diinginkan
    #     cross_tab_combined = cross_tab_combined.stack(level=1).reset_index()
        
    #     # Mengganti nama kolom
    #     cross_tab_combined.columns = ['Region', 'Loan Condition', 'Count', 'Total Amount']
        
    #     # Menambahkan DataFrame laporan ke dalam list
    #     reports.append(cross_tab_combined)
    
    # # Menggabungkan semua laporan dari list menjadi satu DataFrame
    # final_report = pd.concat(reports, ignore_index=True)
    
    # return final_report
# menyimpan ke dalam file excel
        with pd.ExcelWriter(f'report/{periode}.xlsx') as writer:
            cross_tab_count.to_excel(writer, sheet_name = 'Frequency')
            cross_tab_sum.to_excel(writer, sheet_name = 'Total Amount')
            print(f"Berhasil membuat report: report/{periode}.xlsx")


with DAG("dag_lbb",
         start_date = datetime(2024, 8, 29),
         schedule_interval = '*/5 * * * *', # setiap 5 menit
         catchup = False):
    
    fetch_clean_task = PythonOperator(
        task_id = 'fetch_clean_task',
        python_callable = fetch_clean
    )

    df_to_db_task = PythonOperator(
        task_id = 'df_to_db_task',
        python_callable = df_to_db
    )

    report_generator_task = PythonOperator(
        task_id = 'report_generator_task',
        python_callable = report_generator
    )

    fetch_clean_task >> [df_to_db_task, report_generator_task]