import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv('.env')

INPUT_PATH = os.getenv("INPUT_PATH")
OUTPUT_PATH = os.getenv("OUTPUT_PATH")
ERP_FILE = os.getenv("ERP_FILE")
WEB_FILE = os.getenv("WEB_FILE")
LIAISON_FILE = os.getenv("LIAISON_FILE")



def load_transform_to_parquet(file_name, new_file_name):
    pd.read_excel(INPUT_PATH + file_name).astype(str).to_parquet(OUTPUT_PATH + new_file_name + ".parquet",engine="pyarrow")
    print("✅ Load (" + file_name + ") terminé, fichiers prêts pour les étapes suivantes")

load_transform_to_parquet(ERP_FILE, "ERP")
load_transform_to_parquet(WEB_FILE, "WEB")
load_transform_to_parquet(LIAISON_FILE, "LIAISON")

    
    
##python scripts/erp/0_load_raw.py