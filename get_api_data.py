from datetime import datetime
import requests
from sqlalchemy import create_engine
import io
import pandas as pd
import psycopg2


def read_csv(url:str)->pd.DataFrame:
    response = requests.get(url)
    content = response.content
    print(type(content))
    df = pd.read_csv(
        io.BytesIO(content), sep=",", compression="gzip", index_col=0, quotechar='"',
    )
    print(df.shape)
    return df

def table_from_csv(url:str,ym:str,db_name:str,db_login:str,db_password:str,table_name:str,db_host:str='localhost',db_port:int=5432)->None:
    engine = create_engine(f'postgresql://{db_login}:{db_password}@{db_host}:{db_port}/{db_name}')
    df = read_csv(url)
    df.columns = [x.lower() for x in df.columns]
    df['YEAR_MONTH'] = ym
    df.to_sql(table_name, engine,index=False,if_exists='append')

    

def yearmonth(year:str=str(datetime.today().year))->list:
    ym_list = [year + str(month_number) for month_number in range(12,9,-1)] + [year + "0"+ str(month_number) for month_number in range(9,0,-1)] 
    for ym in ym_list:
        url = f'https://opendata.nhsbsa.net/api/3/action/datastore_search?resource_id=EPD_{ym}'
        data = requests.get(url)
        if 'error' not in dict(data.json()).keys():
            return [str(int(ym)-i) for i in range(1,4,1)]


def get_reg_codes(ym)->dict:
    data = requests.get(f'https://opendata.nhsbsa.net/api/3/action/datastore_search_sql?resource_id=EPD_{ym}&sql=SELECT%20DISTINCT%20REGIONAL_OFFICE_CODE%20from%20`EPD_{ym}`')
    return dict(data.json())['result']['result']['records']

def get_file_list(ym,code):
    file_request = requests.get(f'https://opendata.nhsbsa.net/api/3/action/datastore_search_sql?resource_id=EPD_{ym}&sql=SELECT%20*%20from%20`EPD_{ym}`%20WHERE%20REGIONAL_OFFICE_CODE%20=%20%27{code["REGIONAL_OFFICE_CODE"]}%27')
    file_list = dict(file_request.json())['result']['gc_urls']
    print(ym,code["REGIONAL_OFFICE_CODE"],' file_list lenght: ',len(file_list))
    return file_list


def check_table(db_name:str,db_login:str,db_password:str,table_name:str,db_host:str='localhost',db_port:int=5432):
    connection = psycopg2.connect(user=db_login,
                                    password=db_password,
                                    host=db_host,
                                    port=db_port,
                                    database=db_name)
    cursor = connection.cursor()
    cursor.execute(f'''
            SELECT EXISTS (
                SELECT FROM 
                    information_schema.tables 
                WHERE 
                    table_name = '{table_name}'
                )''')
    result = cursor.fetchall()[0]
    cursor.close()
    connection.close()
    return result


def check_data(key_cols:list,db_name:str,db_login:str,db_password:str,table_name:str,db_host:str='localhost',db_port:int=5432):
    connection = psycopg2.connect(user=db_login,
                                    password=db_password,
                                    host=db_host,
                                    port=db_port,
                                    database=db_name)
    if len(key_cols)>1:
        concat = ','.join(key_cols)
        query = f'''
                SELECT DISTINCT CONCAT("{concat}") FROM {table_name}
                '''
    else:
        query = f'''
                SELECT DISTINCT "{key_cols[0]}" FROM {table_name}
                '''
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()[0]
    cursor.close()
    connection.close()
    return result
    


if __name__=="__main__":

    table_name = 'epd_data'
    db_name = 'epd_db'
    postgres_login = 'root'
    postgres_password = 'root'

    ym_list = yearmonth()
    print(ym_list)
    create_table =  not check_table(db_name,postgres_login ,postgres_password,table_name)[0]
    if not create_table:
        persisted_data = check_data(['YEAR_MONTH','REGIONAL_OFFICE_NAME'],db_name,postgres_login ,postgres_password,table_name)

    for ym in [ym_list[0]]:
        codes = get_reg_codes(ym)
        for code in codes:
            file_list = get_file_list(ym,code)
            for file in file_list:
                print('Loading File ',file,' to Postgres')
                table_from_csv(file['url'],ym,db_name,postgres_login ,postgres_password,table_name)

    
    
