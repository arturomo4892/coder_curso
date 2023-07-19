import requests, psycopg2, os
import pandas as pd
from datetime import datetime

def create_tables():
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

    for estado in estados_mexico:
        info = weather_info(estado[1])
        all_info = estado + info
        all_date.append(all_info)

    df = pd.DataFrame(all_date, columns=["id_estado", "nombre", "temperatura", "uv_index", "prob_precipitacion", "humedad", "velocidad_viento_Kmph", "direccion_viento", "descripcion", "zona_cercana", "iluminacion_lunar", "fase_lunar"])
    date_str = datetime.now().strftime("%Y-%m-%d %X")

    df["info_date"] = date_str
    print(df)
    return df


if __name__ == '__main__':
    create_tables()
    main()
