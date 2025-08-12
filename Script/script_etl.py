import pandas as pd

last_year_record = 2023

def get_dataset(year):
    return pd.read_csv("data/raw/atus_anual_csv/conjunto_de_datos/atus_anual_"+str(year)+".csv")

if __name__ == '__main__':
    for year in range(1997, last_year_record+1):
        df_traffic_accidents = get_dataset(year)
        print(df_traffic_accidents.info)