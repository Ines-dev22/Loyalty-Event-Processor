import sqlite3

def run_business_intelligence():
    conn = sqlite3.connect("loyalty_data.db")
    cursor = conn.cursor()
    
    print("\n --- ANALYSE BUSINESS INTELLIGENCE ---")

    # 1. Distribution des types d'utilisateurs
    print("\n👥 Répartition des Users :")
    query_users = "SELECT user_type, COUNT(*) FROM users GROUP BY user_type"
    for row in cursor.execute(query_users):
        print(f"- {row[0].upper()} : {row[1]}")

    # 2. Points cumulés uniquement pour les CUSTOMERS
    print("\n Points cumulés (Type: customer uniquement) :")
    query_pts = """
        SELECT 
            COALESCE(u.name, p.user_id) as identifiant,
            SUM(p.points),
            COALESCE(u.user_type, 'INCONNU') as u_type
        FROM points p
        LEFT JOIN users u ON p.user_id = u.id
        GROUP BY p.user_id
        HAVING u_type = 'customer' OR u_type = 'INCONNU'
    """
    for identifiant, total, u_type in cursor.execute(query_pts):
        suffixe = f" [{u_type}]" if u_type != 'customer' else ""
        print(f"- {identifiant}{suffixe} a totalisé {total} points")
  
    # 3. Analyse des boutiques (Store Performance)
    # Note: Assure-toi d'avoir ajouté la colonne store_id dans ta table transactions
    print("\n Activité par Boutique :")
    try:
        query_store = "SELECT store_id, COUNT(*), SUM(amount) FROM transactions GROUP BY store_id"
        for row in cursor.execute(query_store):
            print(f"- Boutique {row[0]} : {row[1]} ventes | Total: {row[2]}€")
    except:
        print("- (Données boutiques non disponibles)")

    conn.close()
    
def analyze_stores():
    conn = sqlite3.connect("loyalty_data.db")
    cursor = conn.cursor()

    print("\n --- ANALYSE DE RENTABILITÉ DES BOUTIQUES ---")

    query = """
        SELECT 
            store_id, 
            COUNT(event_id) as nb_ventes,
            SUM(amount) as ca_total,
            AVG(amount) as panier_moyen
        FROM transactions
        GROUP BY store_id
        ORDER BY ca_total DESC
    """

    rows = cursor.execute(query).fetchall()
    
    for store, count, total, avg in rows:
        print(f" Boutique : {store}")
        print(f"    CA Total : {total:.2f}€")
        print(f"    Nombre de ventes : {count}")
        print(f"    Panier Moyen : {avg:.2f}€")
        print("-" * 20)

    conn.close()
    
def analyze_customer_habits():
    conn = sqlite3.connect("loyalty_data.db")
    cursor = conn.cursor()

    print("\n --- ANALYSE DES HABITUDES HORAIRES ---")

    # On groupe par heure et on compte le nombre de transactions
    query = """
        SELECT 
            strftime('%H', timestamp) as heure, 
            COUNT(*) as nb_achats
        FROM transactions
        GROUP BY heure
        ORDER BY nb_achats DESC
    """

    rows = cursor.execute(query).fetchall()
    
    if not rows:
        print("Aucune donnée temporelle disponible.")
        return

    # On affiche un petit graphique rudimentaire en texte
    for hour, count in rows:
        barre = "█" * count  # Crée une barre visuelle
        print(f" {hour}h : {barre} ({count} achats)")

    # Conseil business basé sur la première ligne (la plus haute)
    best_hour = rows[0][0]
    print(f"\n CONSEIL : Envoie tes notifications à {best_hour}h, c'est là que tes clients sont les plus actifs !")

    conn.close()
    
def analyze_weekly_habits():
    conn = sqlite3.connect("loyalty_data.db")
    cursor = conn.cursor()
    
    # %w renvoie le jour de la semaine (0=Dimanche, 6=Samedi)
    query = """
        SELECT strftime('%w', timestamp) as jour, COUNT(*) 
        FROM transactions 
        GROUP BY jour 
        ORDER BY jour ASC
    """
    days = ["Dimanche", "Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi"]
    
    print("\n --- ANALYSE PAR JOUR DE LA SEMAINE ---")
    for row in cursor.execute(query):
        nom_jour = days[int(row[0])]
        print(f"{nom_jour} : {'█' * row[1]} ({row[1]} transactions)")
    
    conn.close()
    

def analyze_customer_segments():
    conn = sqlite3.connect("loyalty_data.db")
    cursor = conn.cursor()
    
    print("\n --- SEGMENTATION MARKETING (RFM) ---")

    # Requête pour calculer la Récence, Fréquence et le Montant par utilisateur
    query = """
        SELECT 
            u.name,
            u.id,
            CAST((julianday('now') - julianday(MAX(t.timestamp))) AS INT) as recence,
            COUNT(t.event_id) as frequence,
            SUM(t.amount) as montant_total
        FROM users u
        JOIN transactions t ON u.id = t.user_id
        GROUP BY u.id
    """
    
    rows = cursor.execute(query).fetchall()
    
    for name, uid, r, f, m in rows:
        # Logique de segmentation simplifiée
        if r <= 7 and f >= 5:
            segment = " CHAMPION (Fidèle & Actif)"
        elif r > 30:
            segment = " À RISQUE (Inactif depuis 1 mois)"
        elif f == 1:
            segment = " NOUVEAU (Premier achat)"
        else:
            segment = "OPPORTUNITÉ (Client régulier)"
            
        print(f"- {name} (ID: {uid}) : {segment}")
        print(f"  [R: {r}j | F: {f} achats | M: {m:.2f}€]")

    conn.close()
    
def detect_anomalies():
    conn = sqlite3.connect("loyalty_data.db")
    cursor = conn.cursor()
    
    print("\n  --- SYSTÈME DE DÉTECTION D'ANOMALIES ---")

    # RÈGLE 1 : Accumulation suspecte (Vitesse)
    # Plus de 3 attributions de points en moins de 30 minutes
    query_velocity = """
        SELECT p1.user_id, p1.timestamp, COUNT(p2.event_id) as nb_evenements
        FROM points p1
        JOIN points p2 ON p1.user_id = p2.user_id 
            AND p2.timestamp BETWEEN datetime(p1.timestamp, '-30 minutes') AND p1.timestamp
        GROUP BY p1.event_id
        HAVING nb_evenements >= 3
    """
    
    # RÈGLE 2 : Ratio Points/Montant anormal
    # On cherche les points attribués qui sont > 20% du montant de la transaction
    query_ratio = """
        SELECT t.user_id, t.amount, p.points
        FROM transactions t
        JOIN points p ON t.user_id = p.user_id AND ABS(strftime('%s', t.timestamp) - strftime('%s', p.timestamp)) < 60
        WHERE p.points > (t.amount * 0.5) -- Alerte si plus de 50% du montant en points
    """

    print("\n🚩 Alertes de vélocité (Accumulation rapide) :")
    suspicious_v = cursor.execute(query_velocity).fetchall()
    if not suspicious_v:
        print("  Aucune fraude de vitesse détectée.")
    for row in suspicious_v:
        print(f"  USER {row[0]} : {row[2]} attributions de points en 30min !")

    print("\n🚩 Alertes de ratio (Points disproportionnés) :")
    suspicious_r = cursor.execute(query_ratio).fetchall()
    if not suspicious_r:
        print("  Tous les ratios points/montant semblent cohérents.")
    for row in suspicious_r:
        print(f"  USER {row[0]} : {row[2]} pts pour un achat de {row[1]}€ (Ratio suspect)")

    conn.close()

if __name__ == "__main__":
    run_business_intelligence()
    analyze_stores()
    analyze_customer_habits()
    analyze_weekly_habits()
    analyze_customer_segments()
    detect_anomalies()
    