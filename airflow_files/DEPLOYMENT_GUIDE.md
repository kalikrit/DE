
# ИНСТРУКЦИЯ ПО РАЗВЕРТЫВАНИЮ SBERAUTO ETL В AIRFLOW

## 1. ПОДГОТОВКА СЕРВЕРА
- Установите AirFlow 2.6+
- Установите DuckDB: apt-get install -y duckdb

## 2. СТРУКТУРА ПАПОК
/opt/airflow/
├── dags/
├── plugins/
│   └── sberauto_etl/
│       ├── etl_functions.py
│       ├── create_tables.sql
│       └── __init__.py
├── data/
│   ├── analytics_db.duckdb
│   └── incoming/
│       ├── ga_sessions_new.json
│       └── ga_hits_new.json

## 3. НАСТРОЙКА AIRFLOW
Добавьте в airflow.cfg:
[core]
dags_folder = /opt/airflow/dags
plugins_folder = /opt/airflow/plugins

## 4. РАЗВЕРТЫВАНИЕ
1. Скопируйте файлы из airflow_files/ в соответствующие папки
2. Создайте БД: duckdb /opt/airflow/data/analytics_db.duckdb
3. Запустите инициализацию БД через AirFlow UI
4. Настройте периодичность выполнения DAG

## 5. МОНИТОРИНГ
- Проверяйте логи в AirFlow UI
- Мониторьте размер БД
- Следите за журналом processed_files
