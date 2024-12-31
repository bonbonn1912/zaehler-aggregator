import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
import argparse
from datetime import datetime

def fetch_min_max_records(db_config, date_param):
    query = """
    SELECT 
        DATE(inserted_at) AS date,
        (SELECT Zaehlerstand FROM tuya_zaehler WHERE DATE(inserted_at) = %s ORDER BY inserted_at ASC LIMIT 1) AS first_zaehlerstand,
        (SELECT Zaehlerstand FROM tuya_zaehler WHERE DATE(inserted_at) = %s ORDER BY inserted_at DESC LIMIT 1) AS last_zaehlerstand
    FROM tuya_zaehler
    WHERE DATE(inserted_at) = %s
    LIMIT 1;
    """
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            database=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port']
        )
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, (date_param, date_param, date_param))
            results = cursor.fetchall()
            return results
    except Error as error:
        print("Fehler beim Abrufen der Daten:", error)
        return None
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def fetch_min_max_power_consumption(db_config, date_param, device_id):
    query = """
    WITH datensaetze AS (
        SELECT *
        FROM power_consumption
        WHERE DATE(insertedAt) = %s AND deviceId = %s
    ),
    first_last AS (
        SELECT 
            (SELECT phase1_totalReturned FROM datensaetze ORDER BY insertedAt ASC LIMIT 1) AS first,
            (SELECT phase1_totalReturned FROM datensaetze ORDER BY insertedAt DESC LIMIT 1) AS last
    )
    SELECT * FROM first_last;
    """
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            database=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port']
        )
        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, (date_param, device_id))
            results = cursor.fetchall()
            return results
    except Error as error:
        print("Fehler beim Abrufen der Daten:", error)
        return None
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def upsert_daily_usage(db_config, date, first_value, last_value, name):
    query = """
    INSERT INTO DailyUsage (date, first, last, name)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        first = VALUES(first),
        last = VALUES(last),
        date = VALUES(date);
    """
    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            database=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port']
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(query, (date, first_value, last_value, name))
            connection.commit()
    except Error as error:
        print("Fehler beim Einfügen/Aktualisieren der Daten:", error)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    try:
        load_dotenv()
        db_config = {
            'dbname': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        parser = argparse.ArgumentParser(description="Abrufen von Verbrauchsdaten für ein bestimmtes Datum und Einfügen/Aktualisieren in DailyUsage.")
        parser.add_argument("--date", type=str, help="Das gewünschte Datum im Format YYYY-MM-DD", default=datetime.now().strftime("%Y-%m-%d"))
        args = parser.parse_args()

        date_param = args.date

        # Verarbeitung für tuya_zaehler
        tuya_results = fetch_min_max_records(db_config, date_param)

        if tuya_results:
            first_zaehlerstand = tuya_results[0]['first_zaehlerstand']
            last_zaehlerstand = tuya_results[0]['last_zaehlerstand']
            upsert_daily_usage(db_config, date_param, first_zaehlerstand, last_zaehlerstand, "tuya")

        # Verarbeitung für power_consumption
        for device_id in range(1, 7):
            name = f"Device{device_id}"
            results = fetch_min_max_power_consumption(db_config, date_param, device_id)

            if results and len(results) > 0:
                first_value = results[0]['first']
                last_value = results[0]['last']
                upsert_daily_usage(db_config, date_param, first_value, last_value, name)
    except Exception as e:
        print("Ein Fehler ist aufgetreten:", e)
