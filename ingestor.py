import json
import csv
import logging
from db import bulk_save_users, bulk_save_transactions, bulk_save_points

# Configuration des logs pour garder une trace des erreurs sans arrêter le script
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class EventIngestor:
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.buffers = {"user_created": [], "transaction": [], "points_added": []}
        self.error_count = 0

    def flush(self):
        """Envoie les données en base et gère les erreurs d'écriture."""
        try:
            if self.buffers["user_created"]: bulk_save_users(self.buffers["user_created"])
            if self.buffers["transaction"]: bulk_save_transactions(self.buffers["transaction"])
            if self.buffers["points_added"]: bulk_save_points(self.buffers["points_added"])
        except Exception as e:
            logging.error(f" Échec de l'écriture en base : {e}")
        finally:
            for key in self.buffers: self.buffers[key] = []

    def process_event(self, event_type, data, timestamp):
        """Valide et ajoute l'événement au buffer."""
        if not event_type or not data:
            raise ValueError("Données d'événement incomplètes")
            
        if event_type in self.buffers:
            data["timestamp"] = timestamp
            if "event_id" not in data:
                import uuid
                data["event_id"] = str(uuid.uuid4())[:8] # Un petit ID unique
            if event_type == "transaction":
                amount = float(data.get("amount", 0))
                if amount <= 0:
                    logging.warning(f" Ignoré : Montant invalide ({amount}) pour ID {data['event_id']}")
                    return
            self.buffers[event_type].append(data)
            
            if sum(len(b) for b in self.buffers.values()) >= self.batch_size:
                self.flush()

    def read_txt(self, file_path):
        """Lit un format type 'TYPE|ID|VALEUR|DATE'."""
        with open(file_path, "r") as f:
            for line_num, line in enumerate(f, 1):
                try:
                    parts = line.strip().split('|')
                    if len(parts) < 4: continue 
                    
                    e_type, u_id, val, ts = parts
                    data = {"user_id": u_id, "amount": float(val)} if e_type == "transaction" else {"user_id": u_id, "name": val}
                    self.process_event(e_type, data, ts)
                except Exception as e:
                    self.log_error(line_num, e)

    def read_json(self, file_path):
        with open(file_path, "r") as f:
            try:
                events = json.load(f)
                for i, event in enumerate(events):
                    try:
                        if "event_id" in event:
                            event["data"]["event_id"] = event["event_id"]
                        self.process_event(event["event_type"], event["data"], event["timestamp"])
                    except Exception as e:
                        self.log_error(f"Index {i}", e)
            except json.JSONDecodeError:
                logging.error(" Fichier JSON mal formé (syntaxe invalide).")

    def read_csv(self, file_path):
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                try:
                    e_type = row.pop("event_type")
                    ts = row.pop("timestamp")
                    self.process_event(e_type, row, ts)
                except Exception as e:
                    self.log_error(f"Ligne {i}", e)

    def log_error(self, location, error):
        """Centralise le rapport d'erreurs."""
        self.error_count += 1
        logging.warning(f" Erreur à {location} : {error}")

    def run(self, file_path):
        """Sélecteur de format avec gestion d'erreur globale."""
        try:
            if file_path.endswith(".json"): self.read_json(file_path)
            elif file_path.endswith(".csv"): self.read_csv(file_path)
            elif file_path.endswith(".txt"): self.read_txt(file_path)
            else: logging.error("Format non supporté")
            
            self.flush()
            logging.info(f" Ingestion finie. Erreurs rencontrées : {self.error_count}")
        except Exception as e:
            logging.critical(f" Arrêt critique du pipeline : {e}")