import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def generate_dashboard():
    conn = sqlite3.connect("loyalty_data.db")
    
    # 1. Analyse globale des ventes
    df_sales = pd.read_sql_query("SELECT timestamp, amount FROM transactions", conn)
    # fig_main = px.line(df_sales, x='timestamp', y='amount', title="Flux des ventes en temps réel")
    fig_main = px.line(df_sales, x='timestamp', y='amount', line_shape='hv')
    
    # 2. Analyse par Boutique (Le "Filtre")
    # On récupère la liste des boutiques
    stores = pd.read_sql_query("SELECT DISTINCT store_id FROM transactions", conn)['store_id'].tolist()
    
    store_sections = ""
    for store in stores:
        df_store = pd.read_sql_query(f"SELECT amount FROM transactions WHERE store_id='{store}'", conn)
        total = df_store['amount'].sum()
        count = len(df_store)
        
        store_sections += f"""
        <div class="store-card">
            <h3>Boutique : {store}</h3>
            <p>Chiffre d'affaires : <b>{total:.2f} €</b></p>
            <p>Nombre de transactions : <b>{count}</b></p>
        </div>
        """

    # 3. Génération du HTML
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', sans-serif; background: #f0f2f5; margin: 20px; }}
            .container {{ max-width: 1000px; margin: auto; background: white; padding: 20px; border-radius: 12px; }}
            .store-card {{ border-left: 5px solid #007bff; background: #f8f9fa; padding: 15px; margin: 10px 0; border-radius: 4px; }}
            h1 {{ color: #1a73e8; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1> Dashboard Fidélité Pro</h1>
            <div id="main-chart">{fig_main.to_html(full_html=False, include_plotlyjs='cdn')}</div>
            <hr>
            <h2>Analyse par Point de Vente</h2>
            <div class="grid">{store_sections}</div>
        </div>
    </body>
    </html>
    """
    
    with open("report.html", "w") as f:
        f.write(html_content)
    print("✨ Dashboard généré avec succès !")

if __name__ == "__main__":
    generate_dashboard()