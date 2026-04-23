# main.py
from db import init_db
from ingestor import EventIngestor

def main():
    init_db()
    
    # 2. 
    ingestor = EventIngestor(batch_size=2000)
    
    # 3. 
    ingestor.run("data_sample.csv")
    ingestor.run("events_sample.json")

if __name__ == "__main__":
    main()