import psycopg2
from dotenv import load_dotenv
import os 

load_dotenv()

HOST='localhost'
PORT=5432
DBNAME='urban_scaling_db'

USER=os.environ.get('user')
PASSWORD=os.environ.get('password')

def init_db():
    create_urban_scaling_db()
    create_city_table()
    create_block_table()
    create_service_table()
    create_tag_table()
    create_servicetag_table()
    

def execute_query(
    query,
    host=HOST,
    port=PORT,
    user=USER,
    dbname=DBNAME,
    password=PASSWORD,
    return_rows=None):
    
    connection = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        dbname=dbname,
        password=password)
    connection.autocommit = True
    
    cursor = connection.cursor()

    try:
        if type(query) == list:
            for q in query:
                cursor.execute(q)
        else:
            cursor.execute(query)
        
        if return_rows is not None:
            if return_rows == 'all':
                return cursor.fetchall()
            elif return_rows == 1:
                return cursor.fetchone()
            elif return_rows > 1:
                return cursor.fetchmany(return_rows)
        
    finally:
        cursor.close()
        connection.close()


def create_urban_scaling_db():
    execute_query(f"CREATE DATABASE {DBNAME};",dbname=None)
    execute_query('CREATE EXTENSION postgis')
    

def create_city_table():
    execute_query("""
        CREATE TABLE IF NOT EXISTS city (
            city_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name VARCHAR(255),
            country VARCHAR(255),
            population INT,
            area FLOAT,
            geometry geometry(Multipolygon, 4326)
        );
        """)
    
    
def create_block_table():
    execute_query("""
        CREATE TABLE IF NOT EXISTS block (
            block_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            city_id INT,
            cluster_id INT,
            population INT,
            area FLOAT,
            geometry geometry(Polygon, 4326),
            
            CONSTRAINT fk_city FOREIGN KEY(city_id) REFERENCES city(city_id)
            ON DELETE CASCADE
        );
        """)
    

def create_service_table():
    execute_query("""
        CREATE TABLE IF NOT EXISTS service (
            service_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            city_id INT,
            name VARCHAR(255),
            geometry geometry(Point, 4326),
            
            CONSTRAINT fk_city FOREIGN KEY(city_id) REFERENCES city(city_id)
            ON DELETE CASCADE
        );
        """)
   

def create_tag_table():
    execute_query("""
        CREATE TABLE IF NOT EXISTS tag (
            tag_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            servicetag_id INT,
            name VARCHAR(255),
            category VARCHAR(255)               
        );
        """)
    
    
def create_servicetag_table():
    execute_query([
        """
        CREATE TABLE IF NOT EXISTS servicetag (
            servicetag_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            service_id INT,
            tag_id INT,
            
            CONSTRAINT fk_service FOREIGN KEY(service_id) REFERENCES service(service_id)
            ON DELETE CASCADE,
            
            CONSTRAINT fk_tag FOREIGN KEY(tag_id) REFERENCES tag(tag_id)
        );
        """,
        
        """
        ALTER TABLE tag 
        ADD CONSTRAINT fk_servicetag FOREIGN KEY(servicetag_id) REFERENCES servicetag(servicetag_id)
        """
    ])
    
    
def create_cluster_view():
    execute_query("""
        CREATE VIEW cluster AS
        SELECT 
        FROM
        JOIN
        GROUP BY
    """)