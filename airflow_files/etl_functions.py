import duckdb
import pandas as pd
import json
import hashlib
import os
from datetime import datetime

def calculate_md5(file_path):
    """Вычисляет MD5-хеш файла"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def process_json_file_final(file_path, table_name, db_connection):
    """
    Основная ETL-функция для AirFlow (ИСПРАВЛЕННАЯ ВЕРСИЯ)
    Возвращает: (success, records_processed)
    """
    file_name = os.path.basename(file_path)
    file_md5 = calculate_md5(file_path)
    file_size = os.path.getsize(file_path)

    print(f"🔍 Обрабатываем файл: {file_name}")
    print(f"   MD5: {file_md5}")
    print(f"   Размер: {file_size / 1024:.1f} KB")

    # Проверка на уже обработанный файл
    existing_file = db_connection.execute(f"""
        SELECT * FROM processed_files
        WHERE file_name = '{file_name}' AND md5_hash = '{file_md5}'
    """).fetchone()

    if existing_file:
        print(f"⚠️  Файл уже обработан. Пропускаем.")
        return True, 0

    try:
        # Чтение и парсинг JSON
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        date_key = list(data.keys())[0]
        records_list = data[date_key]
        df = pd.DataFrame(records_list)

        print(f"   Дата в файле: {date_key}")
        print(f"   Записей для обработки: {len(df):,}")

        # Добавляем технические поля
        df['source_file'] = file_name
        df['processed_at'] = datetime.now()

        # Подсчет записей до вставки
        count_before_result = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count_before = count_before_result[0] if count_before_result else 0

        # СОЗДАЕМ ВРЕМЕННУЮ ТАБЛИЦУ (как в Colab)
        db_connection.execute("CREATE OR REPLACE TEMP TABLE temp_new_data AS SELECT * FROM df")

        # ИСПОЛЬЗУЕМ ПРОВЕРЕННЫЙ СИНТАКСИС NOT EXISTS
        if table_name == 'sessions_incremental':
            db_connection.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM temp_new_data t
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} e
                    WHERE e.session_id = t.session_id 
                )
            """)
        else:
            # Для hits проверяем по комбинации полей
            db_connection.execute(f"""
                INSERT INTO {table_name}
                SELECT * FROM temp_new_data t
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} e
                    WHERE e.session_id = t.session_id 
                    AND e.hit_number = t.hit_number
                    
                )
            """)

        # Подсчет добавленных записей
        count_after = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        inserted_count = count_after - count_before

        # Очищаем временную таблицу
        db_connection.execute("DROP TABLE IF EXISTS temp_new_data")

        # Запись в журнал
        db_connection.execute(f"""
            INSERT OR REPLACE INTO processed_files
            VALUES ('{file_name}', {file_size}, {inserted_count}, CURRENT_TIMESTAMP, '{file_md5}')
        """)

        print(f"✅ Успешно. Добавлено записей: {inserted_count:,}")
        return True, inserted_count

    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        # Очищаем временную таблицу в случае ошибки
        db_connection.execute("DROP TABLE IF EXISTS temp_new_data")
        return False, 0

def process_json_file_final_csv(file_path, table_name, db_connection):
    """
    Альтернативная ETL-функция с использованием CSV для обхода бага duckdb
    """
    import tempfile
    import os
    
    file_name = os.path.basename(file_path)
    file_md5 = calculate_md5(file_path)
    file_size = os.path.getsize(file_path)

    print(f"🔍 Обрабатываем файл: {file_name}")
    print(f"   MD5: {file_md5}")
    print(f"   Размер: {file_size / 1024:.1f} KB")

    try:
        # Проверяем, не обрабатывался ли файл ранее
        existing_file = db_connection.execute(f"""
            SELECT * FROM processed_files 
            WHERE file_name = '{file_name}' AND md5_hash = '{file_md5}'
        """).fetchone()

        if existing_file:
            print(f"📁 Файл {file_name} уже обработан, пропускаем")
            return True, 0

        # Загрузка данных из JSON
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        date_key = list(data.keys())[0]
        records_list = data[date_key]
        df = pd.DataFrame(records_list)

        print(f"   Дата в файле: {date_key}")
        print(f"   Записей для обработки: {len(df):,}")

        # Добавляем технические поля
        df['source_file'] = file_name
        df['processed_at'] = datetime.now()

        # Подсчет записей до вставки
        count_before_result = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count_before = count_before_result[0] if count_before_result else 0

        # Используем CSV для обхода бага duckdb
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            csv_path = tmp_file.name
        
        try:
            # Сохраняем DataFrame в CSV
            df.to_csv(csv_path, index=False)
            
            # Создаем временную таблицу из CSV
            db_connection.execute(f"""
                CREATE OR REPLACE TEMP TABLE temp_new_data AS 
                SELECT * FROM read_csv_auto('{csv_path}')
            """)
            
            # Дедупликация и вставка
            if table_name == 'sessions_incremental':
                db_connection.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM temp_new_data t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table_name} e
                        WHERE e.session_id = t.session_id AND e.source_file = t.source_file
                    )
                """)
            else:
                # Для hits используем упрощенную дедупликацию
                db_connection.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM temp_new_data t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table_name} e
                        WHERE e.session_id = t.session_id AND e.hit_number = t.hit_number
                    )
                """)
            
            # Подсчет добавленных записей
            count_after = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            inserted_count = count_after - count_before
            
            # Запись в журнал
            db_connection.execute(f"""
                INSERT OR REPLACE INTO processed_files
                VALUES ('{file_name}', {file_size}, {inserted_count}, CURRENT_TIMESTAMP, '{file_md5}')
            """)
            
            print(f"✅ Успешно. Добавлено записей: {inserted_count:,}")
            return True, inserted_count
            
        finally:
            # Очищаем временный файл
            if os.path.exists(csv_path):
                os.unlink(csv_path)
                
    except Exception as e:
        print(f"❌ Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
        return False, 0
