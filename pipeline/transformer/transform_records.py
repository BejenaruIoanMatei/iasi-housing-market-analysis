import psycopg2
import os
import re

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
    print("Creating table dev.staging_listings if not exists...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dev.staging_listings (
                id SERIAL PRIMARY KEY,
                pret NUMERIC,
                suprafata_utila NUMERIC,
                camere SMALLINT,
                pret_mp NUMERIC,
                zona TEXT,
                vechime_imobil TEXT,
                tip_zona TEXT
            );
        """)
        conn.commit()
        print("Table was created")
    except psycopg2.Error as e:
        print(f"Failed to create table dev.raw_listings: {e}")
        raise
    
def extrage_suprafata(text):
    """Cauta aparitiile (m²|mp|m) si extrage numarul din fata suprafetei
    Extrage suprafata apartamentului

    Args:
        text (string): ..pe metru pătrat | 52 m² | Etaj | 3 etajul ..

    Returns:
        float: Numarul de metri patrati
    """
    match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m²|mp|m)', str(text).lower())
    if match:
        return float(match.group(1))
    return None

def extrage_camere(text):
    """Cauta cifre urmate de camer
    Extrage numarul de camere 

    Args:
        text (string): .. | Numărul de camere | 3 camere | Prețul pe metru pătrat ..

    Returns:
        integer: Numarul de camere al apartamentului
    """
    match = re.search(r'(\d+)\s*camer', str(text).lower())
    if match:
        return int(match.group(1))
    return None

def extrage_an(text):
    """Cauta expresii similare cu "anul 1980", "din 1980", "construit in 1980"
    Validare sa nu fie prea vechi si nici din viitor

    Args:
        text (string): ..bloc cu pod ce a fost construit in anul 1979, acesta fiind fara risc sei...

    Returns:
        integer: Anul constructiei
    """
    match = re.search(r'(?:19|20)\d{2}', str(text))
    if match:
        an = int(match.group(0))
        if an >= 1950 and an <= 2026:
            return an
    return None

def extrage_zona(text):
    """Cauta segmentul care contine "Iasi" si ia ce e inainte de virgula
    "Mircea cel Batran, Iasi, Iasi" -> "Mircea cel Batran"

    Args:
        text (string): "Mircea cel Batran, Iasi, Iasi"

    Returns:
        string: Zona din Iasi, unde este apartamentul, sau Necunoscut
    """
    
    if not isinstance(text, str): return "Necunoscut"
    
    parts = text.split('|')
    for part in parts:
        if 'iasi' in part.lower() and ',' in part:
            zona = part.split(',')[0].strip()
            if len(zona) < 30 and zona.lower() != "iasi":
                return zona
    return "Iasi (General)"

mapping_zone = {
    'Nicolina-CUG': ['Nicolina 1', 'Nicolina 2', 'CUG', 'Hlincea', 'Tudor Neculai', 'Soseaua Nicolina', 'Poitiers', 'Manta Rosie'],
    'Centru-Civic': ['Centru', 'Palas', 'Independentei', 'Academiei', 'Ion Creanga', 'Carol I', 'Anastasie Panu', 'Cuza Voda', 'Arcu', 'Smardan', 'Podu de Fier'],
    'Podu-Ros-Cantemir': ['Podu Ros', 'Cantemir', 'Tesatura'],
    'Tatarasi-Tudor': ['Tatarasi Sud', 'Tatarasi Nord', 'Vasile Lupu', 'Oancea', 'Tudor Vladimirescu', 'Baza 3'],
    'Pacurari-Canta': ['Pacurari', 'Canta', 'Moara de Foc'],
    'Copou-Saras': ['Copou', 'Agronomie', 'Sadoveanu', 'Agronomilor', 'Moara de Vant', 'Ticau'],
    'Alexandru-Dacia': ['Alexandru Cel Bun', 'Dacia', 'Mircea cel Batran', 'Bularga', 'Decebal'],
    'Bucium': ['Bucium', 'Visan', 'Barnova'],
    'Galata-Frumoasa': ['Galata', 'Frumoasa', 'Ciurea', 'Bisericii'],
    'Periferie-Metropolitana': ['Miroslava', 'Rediu', 'Dancu', 'Aroneanu', 'Valea Lupului', 'Voinesti']
}

def clean_location(x):
    x = str(x)
    for categoria, cuvinte_cheie in mapping_zone.items():
        for cuvant in cuvinte_cheie:
            if cuvant.lower() in x.lower():
                return categoria
    return "gunoi"

def tip_de_zona(zona):
    if zona in ['Copou-Saras', 'Centru-Civic']:
        return 'Premium'
    elif zona in ['Tatarasi-Tudor', 'Podu-Ros-Cantemir', 'Pacurari-Canta', 'Alexandru-Dacia']:
        return 'Standard/Urban'
    elif zona in ['Nicolina-CUG', 'Galata-Frumoasa']:
        return 'Accesibil/Rezidential'
    else:
        return 'Periferie'
    
def extrage_pret(text):
    text = str(text).replace('€', '').strip()
    text = re.sub(r'[^\d]', '', text)
    return float(text) if text else None

def transform_and_load(conn):
    print("Transforming and loading data into dev.staging_listings...")
    cursor = conn.cursor()
    cursor.execute("SELECT id, title_raw, price_raw, description_raw FROM dev.raw_listings")
    rows = cursor.fetchall()

    inserted = 0
    skipped = 0

    for row in rows:
        id, title_raw, price_raw, description_raw = row

        pret = extrage_pret(price_raw)
        suprafata = extrage_suprafata(description_raw)
        camere = extrage_camere(description_raw)
        an = extrage_an(description_raw)
        zona_raw = extrage_zona(description_raw)
        zona = clean_location(zona_raw)
        tip_zona = tip_de_zona(zona)

        pret_mp = round(pret / suprafata, 2) if pret and suprafata else None

        if not pret or not suprafata:
            skipped += 1
            continue

        cursor.execute("""
            INSERT INTO dev.staging_listings (
                pret, suprafata_utila, camere, pret_mp, zona, vechime_imobil, tip_zona
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (pret, suprafata, camere, pret_mp, zona, str(an) if an else None, tip_zona))
        inserted += 1

    conn.commit()
    print(f"Done: {inserted} randuri inserate, {skipped} sarite (date lipsa).")


def main():
    try:
        conn = connect_to_db()
        create_table(conn)
        transform_and_load(conn)
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()