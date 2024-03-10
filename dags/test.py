from airflow.decorators import dag, task 
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

import pendulum
import requests
import xmltodict


PODCAST_URL = "https://www.marketplace.org/feed/podcast/marketplace/"

@dag(
    dag_id = 'podcast_summary',
    description='this a data quest tutorial to download the lastet 50 podcast',
    start_date=pendulum.datetime(2024,3,9),
    schedule_interval='@daily',
    catchup=False,
)
def podcast_summary():

    create_db = SqliteOperator(
        sqlite_conn_id='podcasts',
        task_id="create_db",
        sql=r"""
            CREATE TABLE IF NOT EXISTS episodes (
                link TEXT primary key,
                title TEXT,
                filename TEXT,
                published TEXT,
                description TEXT
            )
        """
    )

    @task()
    def get_podcast():
        data = requests.get(PODCAST_URL)
        fetch = xmltodict.parse(data.text)
        episodes = fetch['rss']['channel']['item']
        print(f'length of the episodes {len(episodes)}')
        return episodes
    

    podcast_data = get_podcast()
    create_db.set_downstream(podcast_data)

podcast = podcast_summary()


    


