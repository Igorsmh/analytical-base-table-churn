import kaggle as kg
import pandas as pd



def download_dataset() -> pd.DataFrame:
    """
    Downloads a dataset from Kaggle.

    Returns:
        pd.DataFrame: The downloaded dataset.

    Raises:
        ValueError: If the dataset cannot be downloaded.

    """

    kg.api.authenticate()
    kg.api.dataset_download_files(\
    'teocalvo/analytical-base-table-churn',path=r'data', unzip=True)




def to_parquet() -> pd.DataFrame:
    """
    Reads a CSV file and converts it to a Parquet file.

    Returns:
        pd.DataFrame: The converted Parquet file.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
        ValueError: If the CSV file cannot be read.

    """
   
    data = pd.read_csv(r"data\abt_churn_20240620.csv",sep=';')
    dataset = data.copy()
    
    return dataset.to_parquet(\
    r'C:\Users\igors\Documentos\Estudo_python\Kaggle\Analytical Base Table Churn\data\teocalvo_churn.parquet',\
     engine='pyarrow')
