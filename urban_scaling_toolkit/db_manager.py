import psycopg2
from dotenv import load_dotenv
import os 
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

import utils

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
    
    populate_cities_table()
    populate_tags_table()
    
    
def drop_db(
    host=HOST,
    port=PORT,
    user=USER,
    dbname=DBNAME,
    password=PASSWORD):
    
    post_query(f'DROP DATABASE {dbname}',dbname=None)
    
    
def post_query(
    query,
    host=HOST,
    port=PORT,
    user=USER,
    dbname=DBNAME,
    password=PASSWORD):
    
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
            
    finally:
        cursor.close()
        connection.close()
        

def get_query(
    query,
    host=HOST,
    port=PORT,
    user=USER,
    dbname=DBNAME,
    password=PASSWORD,
    geom=None):
    
    connection = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        dbname=dbname,
        password=password)
        
    if geom is None and 'geom' in query:
        geom=True
    
    if geom:
        res = gpd.read_postgis(query,con=connection,geom_col='geometry',crs=4326)
        return res
    
    res = pd.read_sql(query,con=connection)
    
    connection.close()
    
    return res


def create_urban_scaling_db():
    post_query(f"CREATE DATABASE {DBNAME};",dbname=None)
    post_query('CREATE EXTENSION postgis')
    

def create_city_table():
    post_query("""
        CREATE TABLE IF NOT EXISTS city (
            city_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name VARCHAR(255),
            country VARCHAR(255),
            population INT,
            area FLOAT,
            geometry geometry(Multipolygon, 4326)
        );
        """)
    
    
def create_road_table():
    post_query("""
        CREATE TABLE IF NOT EXISTS road (
            road_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            city_id INT,
            geometry geometry(LineString, 4326),
            
            CONSTRAINT fk_city FOREIGN KEY(city_id) REFERENCES city(city_id)
            ON DELETE CASCADE
        );
        """)
    
    
def create_block_table():
    post_query("""
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
    post_query("""
        CREATE TABLE IF NOT EXISTS service (
            service_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            city_id INT,
            block_id INT,
            name VARCHAR(255),
            geometry geometry(Point, 4326),
            
            CONSTRAINT fk_city FOREIGN KEY(city_id) REFERENCES city(city_id)
            ON DELETE CASCADE,
            
            CONSTRAINT fk_block FOREIGN KEY(block_id) REFERENCES block(block_id)
            ON DELETE CASCADE
        );
        """)
   

def create_tag_table():
    post_query("""
        CREATE TABLE IF NOT EXISTS tag (
            tag_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name VARCHAR(255),
            category VARCHAR(255)              
        );
        """)
    
    
def create_servicetag_table():
    post_query("""
        CREATE TABLE IF NOT EXISTS servicetag (
            servicetag_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            service_id INT,
            tag_id INT,
            
            CONSTRAINT fk_service FOREIGN KEY(service_id) REFERENCES service(service_id)
            ON DELETE CASCADE,
            
            CONSTRAINT fk_tag FOREIGN KEY(tag_id) REFERENCES tag(tag_id)
            ON DELETE CASCADE
        );
        """)
    
    
def create_cluster_view():
    post_query("""
        CREATE VIEW cluster AS
        SELECT 
        FROM
        JOIN
        GROUP BY
    """)
    
    
def populate_tags_table(
    service_tags_path='service_tags.json',
    host=HOST,
    port=PORT,
    user=USER,
    dbname=DBNAME,
    password=PASSWORD):
    
    service_tags_dict = utils.load_json(service_tags_path)
    service_tags = pd.DataFrame(service_tags_dict)
    
    tags = pd.Series({col:service_tags[col].dropna().sum() for col in service_tags.columns}).explode().reset_index()
    tags.columns = ['category','name']
    
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    
    tags.to_sql('tag',con=engine,if_exists='append',index=False)
    
    engine.dispose()
    
    
def populate_cities_table(
    cities_df_path='agglomerations_data.geojson',
    host=HOST,
    port=PORT,
    user=USER,
    dbname=DBNAME,
    password=PASSWORD):
    
    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    
    agglomerations = gpd.read_file(cities_df_path)
    agglomerations.to_postgis('city',con=engine,if_exists='append')
    
    engine.dispose()