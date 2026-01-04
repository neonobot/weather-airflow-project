import os
import sys
from pathlib import Path


project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

from modules.weather_func import get_weather_data 




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_table_sql():
    """SQL для создания таблицы если не существует"""
    return """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        avg_temp FLOAT,
        change_temp FLOAT,
        prediction VARCHAR(12),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

def insert_weather_data():
    weather_data = get_weather_data()
    
    # Подключение к PostgreSQL
    hook = PostgresHook(postgres_conn_id='weather_db') #weather_db  - ID соединения из Airflow Connections
    conn = hook.get_conn() #get_conn() - метод который возвращает live-connection к PostgreSQL
    cursor = conn.cursor() #cursor() - объект для выполнения SQL команд и получения результатов
    

    # Проверка существования таблицы
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'weather_data'
        );
    """)
    table_exists = cursor.fetchone()[0] #fetchone() - получает одну строку



    if table_exists:
        cursor.execute("SELECT avg_temp FROM weather_data ORDER BY date DESC LIMIT 1;")
        result = cursor.fetchone() 
        
        if result:
            previous_temp = result[0]
            today_temp = weather_data['avg_temp']
            
            
            temp_change = round((today_temp - previous_temp),2)
            
            
            if temp_change > 2:
                prediction = 'Потепление'  
            elif temp_change < -2:
                prediction = 'Похолодание'
            else:
                prediction = 'Стабильно'
                
            # Обработка -0
            if temp_change == 0:
                temp_change = 0.0
                
            
            weather_data['change_temp'] = temp_change
            weather_data['prediction'] = prediction
            
        else:
            
            weather_data['change_temp'] = 0.0
            weather_data['prediction'] = ''  # Нет данных для сравнения
            
    else:
        #Если таблицы не существует
        cursor.execute("""
            CREATE TABLE weather_data (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                avg_temp FLOAT,
                change_temp FLOAT,
                prediction VARCHAR(12),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        weather_data['change_temp'] = 0.0
        weather_data['prediction'] = ''  # Первая запись
    
    
    insert_sql = """
        INSERT INTO weather_data (date, avg_temp, change_temp, prediction)
        VALUES (%s, %s, %s, %s)
    """
    
    data = (
        weather_data['date'],
        weather_data['avg_temp'],
        weather_data['change_temp'],
        weather_data['prediction']
    )
    
    cursor.execute(insert_sql, data)
    conn.commit()
    
    cursor.close()
    conn.close()

    
    print(f"Данные сохранены в PostgreSQL: {weather_data['avg_temp']}°C, изменение: {weather_data['change_temp']}°C, прогноз: {weather_data['prediction']}")
    return f"Температура сохранена: {weather_data['avg_temp']}°C"

with DAG(
    'weather_postgres_dag',
    default_args=default_args,
    description='Сохранение данных о погоде в PostgreSQL',
    schedule_interval='25 16 * * *',
    catchup=False,
    tags=['weather', 'postgres'], #теги для фильтрации и группировки задач
) as dag:

    # Задача для создания таблицы
    create_table_task = PostgresOperator(
        task_id='create_weather_table',
        postgres_conn_id='weather_db',
        sql=create_table_sql(),
    )

    # Задача для вставки данных
    insert_data_task = PythonOperator(
        task_id='insert_weather_data',
        python_callable=insert_weather_data,
    )

    # Определяем порядок выполнения
    create_table_task >> insert_data_task