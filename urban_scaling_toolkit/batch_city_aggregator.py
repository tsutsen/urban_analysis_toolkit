import geopandas as gpd
from tqdm import tqdm
from city_model import CityModel
import utils
import db_manager
import data_fetcher



def push_citymodel_to_db(model,city_id,engine,tags_df):
    # push blocks
    max_block_id = db_manager.get_query(f'SELECT MAX(block_id) FROM block')['max'][0]
    if max_block_id is None:
        max_block_id = 0
        
    max_cluster_id = db_manager.get_query(f'SELECT MAX(cluster_id) FROM block')['max'][0]
    if max_cluster_id is None:
        max_cluster_id = 0

    max_service_id = db_manager.get_query(f'SELECT MAX(service_id) FROM service')['max'][0]
    if max_service_id is None:
        max_service_id = 0

    model.blocks['city_id'] = city_id
    model.blocks['cluster_id'] = model.blocks['cluster_id'].astype('Int64')+(max_cluster_id+1)

    old_block_ids = list(model.blocks['block_id'])
    model.blocks['block_id'] += (max_block_id+1)
    new_block_ids = list(model.blocks['block_id'])

    model.blocks[['block_id','city_id','cluster_id','population','area','geometry']].to_postgis(
        'block',con=engine,if_exists='append')


    # push services
    model.services['city_id'] = city_id

    block_id_replace_dict = {x:y for x,y in zip(old_block_ids,new_block_ids)}

    model.services = model.services.drop(['service_id'],axis=1).reset_index().rename(columns={'index':'service_id'})
    model.services['service_id'] += (max_service_id+1)
    model.services['block_id'] = model.services['block_id'].replace(block_id_replace_dict) 

    model.services[['service_id','city_id','block_id','name','geometry']].to_postgis(
        'service',con=engine,if_exists='append')


    # push servicetags
    servicetag = model.services[['service_id','tags']].set_index('service_id')['tags'].explode().reset_index()
    servicetag = servicetag.merge(tags_df,how='inner',left_on='tags',right_on='name')[['service_id','tag_id']]
    servicetag.to_sql('servicetag',con=engine,if_exists='append',index=False)


    # push roads
    model.roads = model.roads.reset_index()[['geometry']]
    model.roads['city_id'] = city_id
    model.roads.to_postgis('road',con=engine,if_exists='append')
    

def batch_process_cities(city_ids,cities_df):
    #city_ids = list(CITIES.query('country==["Brazil"]').sample(100)['city_id']) + list(CITIES.query('country==["Indonesia"]').sample(100)['city_id']) + list(CITIES.query('country==["Japan","Chile"]')['city_id'])

    cities_in_db = db_manager.get_query('SELECT DISTINCT(city_id) FROM block')
    city_ids_to_process = list(set(city_ids)-set(set(cities_in_db['city_id'])))


    for city_id in (pbar := tqdm(city_ids_to_process)):
        try:
            city_name = cities_df[cities_df['city_id']==city_id]['name'].iloc[0]
            pbar.set_description(f"{city_name}")
            
            territory = db_manager.get_query(f"SELECT geometry from city WHERE city_id='{city_id}'",geom=True)
            territory = territory.make_valid()

            pbar.set_description(f"{city_name} [fetching roads]")
            roads = data_fetcher.fetch_roads(territory)
            
            pbar.set_description(f"{city_name} [fetching railways]")
            railways = data_fetcher.fetch_railways(territory)
            
            pbar.set_description(f"{city_name} [fetching water]")
            water = data_fetcher.fetch_water(territory)
            
            pbar.set_description(f"{city_name} [fetching services]")
            services = data_fetcher.fetch_services(territory,verbose=False)

            ###
            pbar.set_description(f"{city_name} [initializing citymodel]")
            model = CityModel(territory,roads,railways,water,verbose=False)

            pbar.set_description(f"{city_name} [generating blocks]")
            model.generate_blocks()
            
            pbar.set_description(f"{city_name} [setting services]")
            model.set_services(services)
            
            pbar.set_description(f"{city_name} [clustering blocks]")
            model.cluster_blocks()
            
            pbar.set_description(f"{city_name} [evaluating centrality]")
            model.evaluate_centrality()
            
            pbar.set_description(f"{city_name} [populating blocks]")
            model.populate_blocks()
            
            pbar.set_description(f"{city_name} [pushing to db]")
            push_citymodel_to_db(model,city_id)
        except:
            continue