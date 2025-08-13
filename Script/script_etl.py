import pandas as pd
import sqlite3

last_year_record = 2023
df_traffic_accidents = pd.DataFrame()

rename_columns = {
    "ID_ENTIDAD": "entidad_id",
    "ID_MUNICIPIO": "municipio_id",
    "TIPACCID": "tipo_accidente",
    "AUTOMOVIL": "automovil",
    "CAMPASAJ": "camioneta_pasajeros",
    "MICROBUS": "microbus",
    "PASCAMION": "camion_urbano",
    "OMNIBUS": "autobus",
    "TRANVIA": "tranvia",
    "CAMIONETA": "camioneta_carga",
    "CAMION": "camion_carga",
    "TRACTOR": "tractor",
    "FERROCARRI": "ferrocarril",
    "MOTOCICLET": "motocicleta",
    "BICICLETA": "bicicleta",
    "OTROVEHIC": "otro_vehiculo",
    "CAUSAACCI": "causa_accidente",
    "CAPAROD": "superficie_rodamiento",
    "SEXO": "sexo",
    "ALIENTO": "aliento_alcoholico",
    "CINTURON": "cinturon",
    "ID_EDAD": "edad",
    "CONDMUERTO":"conductor_muerto",
    "CONDHERIDO": "conductor_herido",
    "PASAMUERTO": "pasajero_muerto",
    "PASAHERIDO": "pasajero_herido",
    "PEATMUERTO": "peaton_muerto",
    "PEATHERIDO": "peaton_herido",
    "CICLMUERTO": "ciclista_muerto",
    "CICLHERIDO": "ciclista_herido",
    "OTROMUERTO": "otro_muerto",
    "OTROHERIDO": "otro_herido",
    "NEMUERTO": "no_espec_muerto",
    "NEHERIDO": "no_espec_herido"
}

def get_dataset(year):
    return pd.read_csv("data/raw/atus_anual_csv/conjunto_de_datos/atus_anual_"+str(year)+".csv")


def create_tables(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vehiculos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        automovil INTEGER,
        camioneta_pasajeros INTEGER,
        microbus INTEGER,
        camion_urbano INTEGER,
        autobus INTEGER,
        tranvia INTEGER,
        camioneta_carga INTEGER,
        camion_carga INTEGER,
        tractor INTEGER,
        ferrocarril INTEGER,
        motocicleta INTEGER,
        bicicleta INTEGER,
        otro_vehiculo INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS heridos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conductor_herido INTEGER,
        pasajero_herido INTEGER,
        peaton_herido INTEGER,
        ciclista_herido INTEGER,
        otro_herido INTEGER,
        no_espec_herido INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS muertos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        conductor_muerto INTEGER,
        pasajero_muerto INTEGER,
        peaton_muerto INTEGER,
        ciclista_muerto INTEGER,
        otro_muerto INTEGER,
        no_espec_muerto INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS entidades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre INTEGER
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS municipios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre INTEGER,
        entidad_id INTEGER,
        FOREIGN KEY (entidad_id) REFERENCES entidades(id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS accidentes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        municipio_id INTEGER,
        tipo_accidente TEXT,
        causa_accidente TEXT,
        superficie_rodamiento TEXT,
        sexo TEXT,
        aliento_alcoholico TEXT,
        cinturon TEXT,
        edad INTEGER,
        fecha_hora TEXT,
        zona TEXT,
        vehiculos_id INTEGER,
        heridos_id INTEGER,
        muertos_id INTEGER,
        FOREIGN KEY (vehiculos_id) REFERENCES vehiculos(id),
        FOREIGN KEY (heridos_id) REFERENCES heridos(id),
        FOREIGN KEY (muertos_id) REFERENCES muertos(id),
        FOREIGN KEY (municipio_id) REFERENCES municipios(id)
    );
    """)

def get_last_index(cursor, table):
    cursor.execute(f"SELECT MAX(id) FROM {table}")
    last_index = cursor.fetchone()[0]    
    if last_index:
        return last_index
    else:
        return 0

if __name__ == '__main__':

    conn = sqlite3.connect("data/DB/accidentes_trafico_mexico")
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")  

    create_tables(cursor) 

    df_entidades = pd.read_csv("data/raw/atus_anual_csv/catalogos/tc_entidad.csv")
    df_municipios = pd.read_csv("data/raw/atus_anual_csv/catalogos/tc_municipio.csv")

    rename_entidades = {
        "ID_ENTIDAD": "id",
        "NOM_ENTIDAD": "nombre"
    }

    for original, new in rename_entidades.items():
        if original in df_entidades.columns:
            df_entidades.rename(columns={original: new}, inplace=True)

    rename_municipios = {
        "ID_ENTIDAD": "entidad_id",
        "ID_MUNICIPIO": "municipio_id",
        "NOM_MUNICIPIO": "nombre"
    }

    for original, new in rename_municipios.items():
        if original in df_municipios.columns:
            df_municipios.rename(columns={original: new}, inplace=True)

    df_municipios.insert(0, "id", range(1, len(df_municipios) + 1))

    df_municipios_load = df_municipios.drop(['municipio_id'], axis=1)

    df_entidades.to_sql("entidades",conn,if_exists="append",index=False)
    df_municipios_load.to_sql("municipios",conn,if_exists="append",index=False)

    for year in range(1997, last_year_record+1):
        try:
            df_traffic_accidents = get_dataset(year)
            print(f"Se encontro el Dataset del año: {year}.")

            df_traffic_accidents["fecha_hora"] = pd.to_datetime(dict(
                    year=df_traffic_accidents["ANIO"],
                    month=df_traffic_accidents["MES"],
                    day=df_traffic_accidents["ID_DIA"],
                    hour=df_traffic_accidents["ID_HORA"],
                    minute=df_traffic_accidents["ID_MINUTO"]
                ), errors='coerce')

            df_traffic_accidents = df_traffic_accidents.dropna()
            df_traffic_accidents = df_traffic_accidents.drop(["ANIO","MES","ID_DIA","ID_HORA","ID_MINUTO"],axis=1)

            zones = pd.DataFrame()
            zones["URBANA"] = df_traffic_accidents["URBANA"].replace("Sin accidente en esta zona", pd.NA)
            zones["SUBURBANA"] = df_traffic_accidents["SUBURBANA"].replace("Sin accidente en esta zona", pd.NA)

            df_traffic_accidents["zona"] = zones["URBANA"].combine_first(zones["SUBURBANA"])
            df_traffic_accidents["zona"] = df_traffic_accidents["zona"].fillna('No especificado')

            for original, new in rename_columns.items():
                if original in df_traffic_accidents.columns:
                    df_traffic_accidents.rename(columns={original: new}, inplace=True)

            df_traffic_accidents = df_traffic_accidents.drop(['COBERTURA', 'DIASEMANA', 'CLASACC', 'ESTATUS'], axis=1)

            print(f"El dataset tiene {len(df_traffic_accidents)} registros")

            df_vehiculos = df_traffic_accidents[['automovil','camioneta_pasajeros', 'microbus', 'camion_urbano', 'autobus','tranvia', 'camioneta_carga', 
                                     'camion_carga', 'tractor', 'ferrocarril', 'motocicleta', 'bicicleta', 'otro_vehiculo']]
            df_heridos = df_traffic_accidents[['conductor_herido', 'pasajero_herido', 'peaton_herido', 'ciclista_herido', 'otro_herido', 'no_espec_herido']]
            df_muertos = df_traffic_accidents[['conductor_muerto', 'pasajero_muerto', 'peaton_muerto', 'ciclista_muerto', 'otro_muerto', 'no_espec_muerto']]
            df_accidentes = df_traffic_accidents[['entidad_id', 'municipio_id', 'tipo_accidente', 'causa_accidente', 'superficie_rodamiento', 'sexo', 
                                                'aliento_alcoholico', 'cinturon', 'edad', 'fecha_hora', 'zona']]
            
            last_index = get_last_index(cursor, "vehiculos")
            df_vehiculos.insert(0, "id", range(last_index + 1, last_index + len(df_vehiculos) + 1))
            df_heridos.insert(0, "id", range(last_index + 1, last_index + len(df_heridos) + 1))
            df_muertos.insert(0, "id", range(last_index + 1, last_index + len(df_muertos) + 1))
            df_accidentes.insert(0, "id", range(last_index + 1, last_index + len(df_accidentes) + 1))

            df_accidentes = df_accidentes.merge(df_municipios[['id', 'entidad_id', 'municipio_id']],on=['entidad_id', 'municipio_id'],how='left')
            df_accidentes = df_accidentes.drop(['entidad_id', 'municipio_id'], axis=1)

            rename_id ={
                "id_x": "id",
                "id_y": "municipio_id"
            }

            for original, new in rename_id.items():
                if original in df_accidentes.columns:
                    df_accidentes.rename(columns={original: new}, inplace=True)

            df_vehiculos.to_sql("vehiculos",conn,if_exists="append",index=False)
            df_muertos.to_sql("muertos",conn,if_exists="append",index=False)
            df_heridos.to_sql("heridos",conn,if_exists="append",index=False)
            df_accidentes.to_sql("accidentes",conn,if_exists="append",index=False)

        except FileNotFoundError:
            print(f"El Dataset del año: {year}, no existe.")
