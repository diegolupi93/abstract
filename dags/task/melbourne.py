import requests
from sqlalchemy import create_engine
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable, TaskInstance

# Dag variables
dag_variables = Variable.get("dag_config", deserialize_json=True)


def classify_category(row: pd.Series) -> str:
    """
    Classifies the price of a property based on quartile ranges.

    Parameters:
    - row (pd.Series): A Pandas Series representing a row of property data with index labels 'Price', '0.25', and '0.75'.

    Returns:
    - str: The classification of the property based on its price:
           - 'economical' if Price < 0.25th percentile value.
           - 'mid_range' if 0.25th percentile <= Price <= 0.75th percentile.
           - 'expensive' if Price > 0.75th percentile.
    """
    if row['Price'] < row['0.25']:
        return 'economical'
    elif row['Price'] >= row['0.25'] and row['Price'] <= row['0.75']:
        return 'mid_range'
    else:
        return 'expensive'


def calculate_percentiles(group: pd.DataFrame, percentiles=[0.25, 0.75]) -> pd.DataFrame:
    """
    Calculate specified percentiles for a group of numeric data.

    Parameters:
    - group (pd.DataFrame): The data group for which percentiles are calculated.
    - percentiles (list): List of percentiles to calculate (default is [0.25, 0.75]).

    Returns:
    - pd.DataFrame: A DataFrame containing the calculated percentiles as columns.
                    Each percentile value is in a separate column, labeled by the percentile.

    """
    return group.quantile(percentiles).to_frame().T



def extract_data(ti: TaskInstance = None, **kwargs) -> None:
    """
    Downloads a file from a URL, saves it locally, and pushes a DataFrame to XCom.

    Parameters:
    - ti (airflow.models.TaskInstance, optional): The task instance object (default is None).
    - **kwargs: Additional keyword arguments passed to the function.

    Raises:
    - RuntimeError: If there is an error during file download or reading the CSV file.
    """
    url = dag_variables['URL']
    download_path = dag_variables['download_path']
    try:
        response = requests.get(url, timeout=20)
        if response.status_code == 200:
            with open(download_path, 'wb') as f:
                f.write(response.content)
        else:
            raise RuntimeError(f'Failed to download file. Status code: {response.status_code}')
    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f'Error downloading file: {e}\n Please download the file manually from {url}'
        ) from e
    df = pd.read_csv('melb_data.csv')
    ti.xcom_push("df", df)



def transform_data(ti: TaskInstance = None, **kwargs) -> None:
    """
    Transform and clean the extracted DataFrame.

    Parameters:
    - ti (airflow.models.TaskInstance, optional): The task instance object (default is None).
    - **kwargs: Additional keyword arguments passed to the function.
    """
    essential_fields = dag_variables['essential_fields']
    df = ti.xcom_pull(task_ids="extract_data", key="df")
    cleaned_df = df.dropna(subset=essential_fields)
    percentiles = [0.25, 0.75]
    percentiles_df = cleaned_df.groupby(['Suburb', 'Rooms', 'Type', 'YearBuilt'])['Price'].quantile(percentiles).unstack()

    percentiles_df.columns = [str(e) for e in percentiles]
    percentiles_df.reset_index(inplace=True)

    cleaned_df = cleaned_df.merge(percentiles_df, on=['Suburb', 'Rooms', 'Type', 'YearBuilt'], how='left')

    cleaned_df['category'] = cleaned_df.apply(classify_category, axis=1)
    cleaned_df.drop(columns=['0.25', '0.75'], inplace=True)
    cleaned_df.columns = cleaned_df.columns.str.lower()

    ti.xcom_push("cleaned_df", cleaned_df)


def load_data(ti: TaskInstance = None, **kwargs) -> None:
    """
    Load cleaned DataFrame into a PostgreSQL database table.

    Parameters:
    - ti (airflow.models.TaskInstance, optional): The task instance object (default is None).
    - **kwargs: Additional keyword arguments passed to the function.
    """
    cleaned_df = ti.xcom_pull(task_ids="transform_data", key="cleaned_df")
    conn = BaseHook.get_connection('postgres_localhost')
    engine = create_engine(f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    with engine.connect() as connection:
        print(f'postgresql+psycopg2://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        cleaned_df.to_sql('melbourne', connection, if_exists='replace', index=False)
