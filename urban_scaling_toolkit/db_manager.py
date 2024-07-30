import warnings
import psycopg2
from dotenv import load_dotenv
import os 
import geopandas as gpd
warnings.filterwarnings('ignore',category=UserWarning)

import pandas as pd
warnings.filterwarnings('ignore',category=UserWarning)

from sqlalchemy import create_engine
warnings.filterwarnings('ignore',category=UserWarning)

import utils

load_dotenv()

HOST='localhost'
PORT=5432
DBNAME='urban_scaling_db_02'

USER=os.environ.get('user')
PASSWORD=os.environ.get('password')

def init_db(dbname=DBNAME):
    create_urban_scaling_db(dbname=dbname)
    create_city_table()
    create_block_table()
    create_road_table()
    create_railway_table()
    create_water_table()
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


def create_urban_scaling_db(dbname=DBNAME):
    post_query(f"CREATE DATABASE {dbname};",dbname=None)
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


def create_railway_table():
    post_query("""
        CREATE TABLE IF NOT EXISTS railway (
            railway_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            city_id INT,
            geometry geometry(LineString, 4326),
            
            CONSTRAINT fk_city FOREIGN KEY(city_id) REFERENCES city(city_id)
            ON DELETE CASCADE
        );
        """)
    

def create_water_table():
    post_query("""
        CREATE TABLE IF NOT EXISTS water (
            water_id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            city_id INT,
            geometry geometry(Geometry, 4326),
            
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
    cities_df_path='agglomerations.geojson',
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
  
    
def create_shannon_diversity_view():
    post_query("""
               CREATE VIEW shannon_diversity AS
                --  THIS QUERY ADDS SHANNON DIVERSITY MAX-SCALED BY MOST DIVERSE CLUSTER IN A CITY
                    SELECT
                        cluster_id,
                        city_id,
                        diversity,
                        diversity/(MAX(diversity) OVER (PARTITION BY city_id)) as diversity_scaled
                    FROM
                    (
                        SELECT
                    -- 		THIS QUERY RETURNS SHANNON DIVERSITY FOR EVERY CLUSTER
                            cluster_id,
                            city_id,
                            -sum(p*ln(p)) as diversity
                        FROM 
                            (
                    -- 			THIS QUERY RETURNS PROPORTIONS OF EACH TAG IN A CLUSTER
                                SELECT
                                block.cluster_id,
                                block.city_id,
                                COUNT(servicetag.tag_id)/(SUM(COUNT(servicetag.tag_id)) OVER (PARTITION BY block.cluster_id)) as p
                                FROM
                                service
                                JOIN servicetag ON service.service_id = servicetag.service_id
                                JOIN block ON block.block_id = service.block_id
                                GROUP BY
                                block.city_id, block.cluster_id, servicetag.tag_id
                            )
                        GROUP BY city_id, cluster_id
                    )
               """)
    

def create_simpson_diversity_view():
    post_query("""
               CREATE VIEW simpson_diversity AS
                -- THIS QUERY ADDS SIMPSON DIVERSITY MAX-SCALED BY MOST DIVERSE CLUSTER IN A CITY
                SELECT
                    cluster_id,
                    city_id,
                    diversity,
                    diversity/(MAX(diversity) OVER (PARTITION BY city_id)) as diversity_scaled
                FROM
                (
                    -- 		THIS QUERY RETURNS SIMPSON DIVERSITY FOR EVERY CLUSTER
                    SELECT
                        cluster_id,
                        city_id,
                        1-(SUM(tags_of_type*(tags_of_type-1)))/(tags_in_cluster*(tags_in_cluster-1)) as diversity
                    FROM
                    (
                        -- 			
                        SELECT
                        block.cluster_id,
                        block.city_id,
                        servicetag.tag_id,
                        COUNT(servicetag.tag_id) as tags_of_type,
                        SUM(COUNT(servicetag.tag_id)) OVER (PARTITION BY block.cluster_id) as tags_in_cluster
                        FROM
                        service
                        JOIN servicetag ON service.service_id = servicetag.service_id
                        JOIN block ON block.block_id = service.block_id
                        GROUP BY
                        block.city_id, block.cluster_id, servicetag.tag_id
                    )
                    WHERE 
                        tags_in_cluster > 1
                    GROUP BY 
                        cluster_id,city_id,tags_in_cluster
                )
               """)
    
    
def create_service_count_view():
    post_query("""
               CREATE VIEW service_count AS
                SELECT
                        block.cluster_id,
                        block.city_id,
                        COUNT(service.service_id) as services_total,
                        CAST(COUNT(service.service_id) AS FLOAT)/(MAX(COUNT(service.service_id)) OVER (PARTITION BY block.city_id)) as services_scaled
                    FROM
                        service
                        JOIN block 
                        ON block.block_id = service.block_id
                    WHERE 
                        cluster_id IS NOT NULL
                    GROUP BY
                        block.city_id, block.cluster_id
               """)
    
    
def create_amenity_saturation_view():
    post_query("""
                CREATE VIEW amenity_saturation AS
                SELECT
                    cluster_id,
                    city_id,
                    diversity_shannon,
                    diversity_shannon_scaled,
                    diversity_simpson,
                    diversity_simpson_scaled,
                    services_total,
                    services_scaled,
                    amenity_saturation_shannon,
                    amenity_saturation_shannon/(MAX(amenity_saturation_shannon) OVER (PARTITION BY city_id)) as amenity_saturation_shannon_scaled,
                    amenity_saturation_simpson,
                    amenity_saturation_simpson/(MAX(amenity_saturation_simpson) OVER (PARTITION BY city_id)) as amenity_saturation_simpson_scaled
                FROM
                (
                    SELECT
                        dshan.cluster_id,
                        dshan.city_id,
                        s.services_total,
                        s.services_scaled,
                        dshan.diversity as diversity_shannon,
                        dshan.diversity_scaled as diversity_shannon_scaled,
                        dsimp.diversity as diversity_simpson,
                        dsimp.diversity_scaled as diversity_simpson_scaled,
                        services_scaled+dshan.diversity_scaled as amenity_saturation_shannon,
                        services_scaled+dsimp.diversity_scaled as amenity_saturation_simpson
                    FROM 
                        shannon_diversity dshan
                        JOIN simpson_diversity dsimp ON dsimp.cluster_id = dshan.cluster_id
                        JOIN service_count s ON dshan.cluster_id = s.cluster_id
                )
                """)
    
    
def create_cluster_view():
    post_query("""
               CREATE VIEW cluster AS
                SELECT
                    a.cluster_id,
                    a.city_id,
                    c.name,
                    c.country,
                    b.population,
                    b.area,
                    a.diversity_shannon,
                    a.diversity_shannon_scaled,
                    a.diversity_simpson,
                    a.diversity_simpson_scaled,
                    a.services_total,
                    a.services_scaled,
                    a.amenity_saturation_shannon,
                    a.amenity_saturation_shannon_scaled,
                    a.amenity_saturation_simpson,
                    a.amenity_saturation_simpson_scaled
                FROM 
                    amenity_saturation a
                    JOIN (
                        SELECT city_id,
                        name,
                        country 
                        FROM city) c ON a.city_id = c.city_id
                    JOIN (
                        SELECT 
                        cluster_id,
                        SUM(population) as population,
                        SUM(area) as area 
                        FROM block 
                        GROUP BY cluster_id) b ON a.cluster_id = b.cluster_id
               """)