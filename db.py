import sqlite3
import logging

DB_NAME = "loyalty_data.db"

def get_connection():
    """Crée une connexion et configure SQLite pour la vitesse."""
    conn = sqlite3.connect(DB_NAME)
    # Optimisations spéciales pour les gros volumes :
    conn.execute("PRAGMA synchronous = OFF") # Ne pas attendre le disque à chaque écriture
    conn.execute("PRAGMA journal_mode = WAL") # Autorise lectures et écritures simultanées
    return conn

def init_db():
    """Crée les tables si elles n'existent pas."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            user_type TEXT,
            name TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            event_id TEXT PRIMARY KEY,
            user_id TEXT,
            amount REAL,
            store_id TEXT,
            timestamp TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS points (
            event_id TEXT PRIMARY KEY,
            user_id TEXT,
            points INTEGER,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()

# --- Fonctions de sauvegarde massive ---

def bulk_save_users(users_list):
    """Insère un lot d'utilisateurs. Ignore les doublons."""
    conn = get_connection()
    try:
        data = [(u['user_id'], u.get('user_type'), u.get('name')) for u in users_list]
        conn.executemany("INSERT OR IGNORE INTO users VALUES (?, ?, ?)", data)
        conn.commit()
    finally:
        conn.close()

def bulk_save_transactions(transactions_list):
    """Insère un lot de transactions."""
    conn = get_connection()
    try:
        data = [(t['event_id'], t['user_id'], t['amount'], t.get('store_id', 'ONLINE'), t['timestamp']) for t in transactions_list]
        conn.executemany("INSERT OR IGNORE INTO transactions VALUES (?, ?, ?, ?, ?)", data)
        conn.commit()
    finally:
        conn.close()

def bulk_save_points(points_list):
    """Insère un lot de points."""
    conn = get_connection()
    try:
        data = [(p['event_id'], p['user_id'], p['points'], p['timestamp']) for p in points_list]
        conn.executemany("INSERT OR IGNORE INTO points VALUES (?, ?, ?, ?)", data)
        conn.commit()
    finally:
        conn.close()


def get_user_balance(user_id):
    """Calcule le solde total des points pour un utilisateur donné."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # SQL fait la somme pour nous, c'est ultra rapide
    query = "SELECT SUM(points) FROM points WHERE user_id = ?"
    cursor.execute(query, (user_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    # Si l'utilisateur n'a pas de points, SUM renvoie None, on transforme en 0
    return result[0] if result[0] is not None else 0