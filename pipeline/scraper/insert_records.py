from scraper import scrape_page
import psycopg2
import os

def connect_to_db():
    print("Connectiong to the PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "db"),
            port=5432,
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise
    
def create_table(conn):
    print("Creating table if now exists...")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_listings (
                id SERIAL PRIMARY KEY,
                title_raw TEXT,
                price_raw TEXT,
                description_raw TEXT,
                url TEXT,
                scraped_at TIMESTAMP
            );
        """
        )
        conn.commit()
        print("Table was created")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise

def insert_records(conn, records: list[dict]):
    print("Inserting raw scraped data into the database...")
    try:
        cursor = conn.cursor()
        for r in records:
            cursor.execute(
                """
                INSERT INTO dev.raw_listings (
                    title_raw,
                    price_raw,
                    description_raw,
                    url,
                    scraped_at
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                r['title_raw'],
                r['price_raw'],
                r['description_raw'],
                r['url'],
                r['scraped_at']
            ))
        conn.commit()
        print("Data successfully inserted")
    except psycopg2.Error as e:
        print(f"Error inserting data into the database: {e}")
        raise

def main():
    try:
        records = scrape_page(1)
        conn = connect_to_db()
        create_table(conn)
        insert_records(conn, records)
    except Exception as e:
        print(f"An error occured during execution: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")

main()