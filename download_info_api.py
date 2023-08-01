import requests, psycopg2, os
import pandas as pd
from datetime import datetime

def create_tables():
    """
    Crea la tabla en la base de datos en caso de que no exista
    """
    create_table = """
            CREATE TABLE IF NOT EXISTS arturomontesdeoca4892_coderhouse.clima_mexico
            (id_estado VARCHAR(5) NOT NULL PRIMARY KEY,
            nombre VARCHAR(20) NOT NULL,
            temperatura NUMERIC(15,2) NOT NULL,
            uv_index NUMERIC(15,2) NOT NULL,
            prob_precipitacion NUMERIC(15,2) NOT null,
            humedad NUMERIC(15,2) not null,
            velocidad_viento_Kmph NUMERIC(15,2) not null,
            direccion_viento VARCHAR(5) not null,
            descripcion VARCHAR(100) not null,
            zona_cercana VARCHAR(60) not null,
            iluminacion_lunar NUMERIC(15,2) not null,
            fase_lunar VARCHAR(30) not null,
            info_date timestamp not null);
            """
    conn = psycopg2.connect(os.getenv["redshift_string_connection"])
    cur = conn.cursor()
    cur.execute(create_table)
    conn.commit()
    conn.close()


def weather_info(city):
    """
    Extrae la informacion de los datos de la ciudad establcida

    args:
        city (str) : Ciudad objetio para extraer datos climaticos del momento
    
    return:
        result (dict) : Contiene la información climatica de la ciudad establecida
    """
    url = f"https://es.wttr.in/{city}?format=j1"
    response = requests.get(url)
    if response.status_code == 200:
        weather = response.json()
        temperatura = int(weather["current_condition"][0]["temp_C"])
        uvIndex = int(weather["current_condition"][0]["uvIndex"])
        prob_precipitacion = float(weather["current_condition"][0]["precipMM"])
        humedad = float(weather["current_condition"][0]["humidity"])
        velocidad_viento_Kmph = int(weather["current_condition"][0]["windspeedKmph"])
        direccion_viento = weather["current_condition"][0]["winddir16Point"]
        descripcion = weather["current_condition"][0]["lang_es"][0]["value"]
        zona_cercana = weather["nearest_area"][0]["areaName"][0]["value"]
        iluminacion_lunar = float(weather["weather"][0]["astronomy"][0]["moon_illumination"])
        moon_phase = weather["weather"][0]["astronomy"][0]["moon_illumination"]

        result = (temperatura, uvIndex, prob_precipitacion, humedad, velocidad_viento_Kmph, direccion_viento, descripcion, zona_cercana, iluminacion_lunar, moon_phase)
        
        return result
    else:
        print(f'Error al conectar con la API. Código de error: {response.status_code}')

def main():
    """
    Funcion principal que obtiene la informacion climatica de todos los estados de México
    """
    estados_mexico = [
        ("AGS", "Aguascalientes"),
        ("BC", "Baja California"),
        ("BCS", "Baja California Sur"),
        ("CAM", "Campeche"),
        ("COAH", "Coahuila"),
        ("COL", "Colima"),
        ("CHIS", "Chiapas"),
        ("CHIH", "Chihuahua"),
        ("CDMX", "Ciudad de México"),
        ("DGO", "Durango"),
        ("GTO", "Guanajuato"),
        ("GRO", "Guerrero"),
        ("HGO", "Hidalgo"),
        ("JAL", "Jalisco"),
        ("MEX", "México"),
        ("MICH", "Michoacán"),
        ("MOR", "Morelos"),
        ("NAY", "Nayarit"),
        ("NL", "Nuevo León"),
        ("OAX", "Oaxaca"),
        ("PUE", "Puebla"),
        ("QRO", "Querétaro"),
        ("QROO", "Quintana Roo"),
        ("SLP", "San Luis Potosí"),
        ("SIN", "Sinaloa"),
        ("SON", "Sonora"),
        ("TAB", "Tabasco"),
        ("TAMPS", "Tamaulipas"),
        ("TLAX", "Tlaxcala"),
        ("VER", "Veracruz"),
        ("YUC", "Yucatán"),
        ("ZAC", "Zacatecas")
    ]

    all_date = []

    # Se obtiene la información de ada estado y se agrega a la lista all_dates
    for estado in estados_mexico:
        info = weather_info(estado[1])
        all_info = estado + info
        all_date.append(all_info)

    # Se construye el dataframe y se agrega la columna info_date con la fecha del momento de ejecución
    df = pd.DataFrame(all_date, columns=["id_estado", "nombre", "temperatura", "uv_index", "prob_precipitacion", "humedad", "velocidad_viento_Kmph", "direccion_viento", "descripcion", "zona_cercana", "iluminacion_lunar", "fase_lunar"])
    date_str = datetime.now().strftime("%Y-%m-%d %X")
    df["info_date"] = date_str
    df = df.fillna(" ", inplace=True)
    df = df.drop_duplicates(inplace=True)


    # Se establace la conexión a la base de datos
    conn = psycopg2.connect(os.getenv["redshift_string_connection"])
    # Se convierte el dataframe a una lista de tuplas donde cada una de ellas contiene los valores de cada registro ej. = ('AGS', 'Aguascalientes', 29, 7, 0., 29., 19, 'E', 'Parcialmente nublado', 'El Taray', 99., '99', '2023-08-01 14:12:58')
    records = df.to_records(index=False)
    data_tuples = list(records)
    # Se obtiene la lista de columnas
    cols = ', '.join(df.columns)
    cursor = conn.cursor()

    # Se itera sobre cada registro y se construye un query para insertar el registro en la base de datos
    for record in data_tuples:
        query = f"INSERT INTO clima_mexico ({cols}) VALUES {record}"
        cursor.execute(query, data_tuples)
        conn.commit()

    print("Dataframe insertado en la base de datos")
    # Cerrar el cursor y la conexión
    conn.close()
    cursor.close()


if __name__ == '__main__':
    create_tables()
    main()
