from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import sys
import os
import glob

# Добавляем путь к плагинам
sys.path.insert(0, '/opt/airflow/plugins')

# Импортируем модули
try:
    from etl_functions import process_json_file_final_csv
    import duckdb
    MODULES_LOADED = True
    print("✅ Модули успешно загружены")
except ImportError as e:
    print(f"❌ Ошибка загрузки модулей: {e}")
    MODULES_LOADED = False

def init_database():
    """Инициализация базы данных"""
    if not MODULES_LOADED:
        return "Modules not loaded"
    
    try:
        conn = duckdb.connect('/opt/airflow/data/analytics_db.duckdb')
        
        # Проверяем существование таблиц, если нет - создаем
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        if 'sessions_incremental' not in table_names:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS sessions_incremental (
                    session_id VARCHAR,
                    client_id VARCHAR,
                    visit_date DATE,
                    visit_time TIME,
                    visit_number INTEGER,
                    utm_source VARCHAR,
                    utm_medium VARCHAR,
                    utm_campaign VARCHAR,
                    utm_adcontent VARCHAR,
                    utm_keyword VARCHAR,
                    device_category VARCHAR,
                    device_os VARCHAR,
                    device_browser VARCHAR,
                    device_screen_resolution VARCHAR,
                    geo_country VARCHAR,
                    geo_city VARCHAR,
                    source_file VARCHAR,
                    processed_at TIMESTAMP
                )
            ''')
        
        if 'hits_incremental' not in table_names:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS hits_incremental (
                    session_id VARCHAR,
                    hit_date DATE,
                    hit_time INTEGER,
                    hit_number INTEGER,
                    hit_type VARCHAR,
                    hit_referer VARCHAR,
                    hit_page_path VARCHAR,
                    event_category VARCHAR,
                    event_action VARCHAR,
                    event_label VARCHAR,
                    event_value INTEGER,
                    source_file VARCHAR,
                    processed_at TIMESTAMP
                )
            ''')
        
        if 'processed_files' not in table_names:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS processed_files (
                    file_name VARCHAR PRIMARY KEY,
                    file_size INTEGER,
                    records_processed INTEGER,
                    processed_at TIMESTAMP,
                    md5_hash VARCHAR
                )
            ''')
        
        conn.close()
        return f"Database initialized. Tables: {len(tables)} -> {conn.execute('SHOW TABLES').fetchall()}"
        
    except Exception as e:
        return f"Database init error: {str(e)}"

def process_sessions_data():
    """Обработка данных сессий"""
    if not MODULES_LOADED:
        return "Modules not loaded"
    
    try:
        conn = duckdb.connect('/opt/airflow/data/analytics_db.duckdb')
        sessions_files = glob.glob('/opt/airflow/data/incoming/ga_sessions_new_*.json')
        total_processed = 0
        
        print(f"Найдено файлов sessions: {len(sessions_files)}")
        
        for file_path in sorted(sessions_files):
            print(f"Обрабатываю: {os.path.basename(file_path)}")
            success, count = process_json_file_final_csv(
                file_path=file_path,
                table_name='sessions_incremental',
                db_connection=conn
            )
            
            if success:
                total_processed += count
                print(f"✅ Успешно: {count} записей")
            else:
                conn.close()
                return f"❌ Ошибка обработки: {file_path}"
        
        conn.close()
        return f"✅ Обработано sessions: {total_processed} записей из {len(sessions_files)} файлов"
        
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

def process_hits_data():
    """Обработка данных хитов"""
    if not MODULES_LOADED:
        return "Modules not loaded"
    
    try:
        conn = duckdb.connect('/opt/airflow/data/analytics_db.duckdb')
        hits_files = glob.glob('/opt/airflow/data/incoming/ga_hits_new_*.json')
        total_processed = 0
        
        print(f"Найдено файлов hits: {len(hits_files)}")
        
        for file_path in sorted(hits_files):
            print(f"Обрабатываю: {os.path.basename(file_path)}")
            success, count = process_json_file_final_csv(
                file_path=file_path,
                table_name='hits_incremental',
                db_connection=conn
            )
            
            if success:
                total_processed += count
                print(f"✅ Успешно: {count} записей")
            else:
                conn.close()
                return f"❌ Ошибка обработки: {file_path}"
        
        conn.close()
        return f"✅ Обработано hits: {total_processed} записей из {len(hits_files)} файлов"
        
    except Exception as e:
        return f"❌ Ошибка: {str(e)}"

# Создаем DAG
with DAG(
    'sberauto_etl_dag',
    description='ETL Pipeline for SberAuto Analytics',
    schedule=None,  # Запуск вручную
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sberauto', 'etl']
) as dag:

    start_task = EmptyOperator(task_id='start')
    
    init_db_task = PythonOperator(
        task_id='initialize_database',
        python_callable=init_database,
    )
    
    process_sessions_task = PythonOperator(
        task_id='process_sessions_data',
        python_callable=process_sessions_data,
    )
    
    process_hits_task = PythonOperator(
        task_id='process_hits_data',
        python_callable=process_hits_data,
    )
    
    end_task = EmptyOperator(task_id='end')
    
    # Определяем порядок выполнения
    start_task >> init_db_task >> process_sessions_task >> process_hits_task >> end_task
