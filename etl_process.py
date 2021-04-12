
import pandas as pd
import logging
import os

from sqlalchemy.engine import create_engine

DATA_PATH = 'Data'

class DataETLManager:
    def __init__(self, root_dir: str, csv_file: str):
        if os.path.exists(root_dir):
            if csv_file.endswith('.csv'):
                self.csv_file = os.path.join(root_dir, csv_file)
            else:
                logging.error('The file is not in csv format')
                exit(1)
        else:
            logging.error('The root dir path does not exist')
            exit(1)

        self.credit_scoring_df = pd.read_csv(self.csv_file, sep=',', encoding='ISO-8859-1')


    def extract_data(self):
        return self.credit_scoring_df

    def fetch_columns(self):
        return self.credit_scoring_df.columns.tolist()

    def data_description(self):
        return self.credit_scoring_df.describe()

    def fetch_categorical(self, categorical=False):
        if categorical:
            categorical_columns = list(set(self.credit_scoring_df.columns) - set(self.credit_scoring_df._get_numerical_data().columns))
            categorical_df = self.credit_scoring_df[categorical_columns]
            return categorical_df
        else:
            non_categorical = list(set(self.credit_scoring_df._get_numerical_data().columns))
            return self.credit_scoring_df[non_categorical]

    def transform_data(self):

        # 1 Drop Duplicates:
        self.credit_scoring_df.drop_duplicates(keep='last', inplace=True)

        # 2 Drop null values:
        self.credit_scoring_df.dropna(how='all', inplace=True)

        # Remove Outliers:
        clean_credit = self.credit_scoring_df.loc[self.credit_scoring_df['Revolving'] <= 1]
        clean_credit = clean_credit.loc[clean_credit['DbtRatio'] <= 1]
        clean_credit = clean_credit.loc[clean_credit['Age'] <= 100]
        clean_credit = clean_credit.loc[clean_credit['Age'] >= 18]
        clean_credit = clean_credit.loc[clean_credit['FamMemb'] < 20]



        # data = self.credit_scoring_df
        #
        # # Checking and eliminating redundant information:
        # data.drop_duplicates(keep='last', inplace=True)
        #
        # # Fill null Values:
        # data['InvoiceNo'].fillna(value=0, inplace=True)
        # data['Description'].fillna(value='No Description', inplace=True)
        # data['StockCode'].fillna(value='----', inplace=True)
        # data['Quantity'].fillna(value=0, inplace=True)
        # data['InvoiceDate'].fillna(value='00/00/0000 00:00', inplace=True)
        # data['UnitPrice'].fillna(value=0.00, inplace=True)
        #
        # data['CustomerID'].fillna(value=0, inplace=True)
        # data['Country'].fillna(value='None', inplace=True)
        #
        # # Format value columns:
        # data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])
        #
        # self.data_transfomed = data


    def load_data(self):
        database_config = {
            'username': 'your_username',
            'password': 'your_pass',
            'host': '127.0.0.1',
            'port':'3306',
            'database':'db_name'
        }

        # create database connection using engine:
        engine = create_engine('mysql+mysqlconnector://{}:{}@{}:{}/{}'.format(
            database_config['username'],
            database_config['password'],
            database_config['host'],
            database_config['port'],
            database_config['database']
        ))

        data_to_load = type(pd.DataFrame())(self.credit_scoring_df)
        try:
            data_to_load.to_sql('Credit Scoring', con=engine, if_exists='append', index=False)
        except Exception as err:
            print(err)
