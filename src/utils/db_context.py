from sqlalchemy import create_engine
import platform
from dotenv import load_dotenv
import os


def mssql_connector(db_name):
    env_file_path = os.path.join(os.path.dirname(__file__), '../../envlocal/.env')
    load_dotenv(dotenv_path=env_file_path)

    user_mssql = os.getenv("MSSQL_USER")
    pass_mssql = os.getenv("MSSQL_PASS")
    port_mssql = os.getenv("MSSQL_PORT")
    server_name_mssql = os.getenv("MSSQL_HOST")


    if db_name.lower() == "ds":
        database_name_mssql_ds = os.getenv("MSSQL_DB_DS")
        # Local
        if platform.system() == 'Windows':
            connection_string_mssql = f"mssql+pyodbc://{server_name_mssql}/{database_name_mssql_ds}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
        # Server
        else:
            connection_string_mssql = f'mssql+pyodbc://{user_mssql}:{pass_mssql}@{server_name_mssql}/{database_name_mssql_ds}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'

    elif db_name.lower() == "dw":
        database_name_mssql_dw = os.getenv("MSSQL_DB_DW")

        # Local
        if platform.system() == 'Windows':
            connection_string_mssql = f'mssql+pyodbc://@{server_name_mssql}/{database_name_mssql_dw}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes'
        # Server
        else:
            connection_string_mssql = f'mssql+pyodbc://{user_mssql}:{pass_mssql}@{server_name_mssql}/{database_name_mssql_dw}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes'

    engine_mssql = create_engine(connection_string_mssql)
    return engine_mssql
