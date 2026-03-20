import duckdb
import os
from dotenv import load_dotenv

load_dotenv('.env')

#je créer un repertoire temp
raw_file = "data/silver/LIAISON.parquet"
clean_file = "data/tmp/link_clean.parquet"
dedup_file = "data/tmp/link_dedup.parquet"

# Connecter DuckDB
con = duckdb.connect(database=':memory:')
# ----------------------------
# 1️⃣ Clean : cast + suppress invalid: product_id casté en INTEGER → les valeurs invalides deviennent NULL.
# ----------------------------
print("🔹 Cleaning link_raw...")

con.execute(f"""
CREATE OR REPLACE TABLE link_clean AS
SELECT
    TRY_CAST(product_id AS INTEGER) AS product_id,
    TRY_CAST(id_web AS INTEGER) AS id_web,
FROM read_parquet('{raw_file}');
""")

# # Stats
# stats = con.execute("""
# SELECT 
#     COUNT(*) AS total_raw,
#     COUNT(product_id) AS valid_product_id,
#     COUNT(*) - COUNT(product_id) AS rejected_product_id
# FROM link_clean
# """).fetchone()

# print(f"Total rows: {stats[0]}, valid product_id: {stats[1]}, rejected: {stats[2]}")

# Export clean
con.execute(f"COPY link_clean TO '{clean_file}' (FORMAT PARQUET)")

# ----------------------------
# 2️⃣ Dedup
# ----------------------------
print("Deduplicating...")

con.execute(f"""
CREATE OR REPLACE TABLE link_dedup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY id_web) AS rn
    FROM read_parquet('{clean_file}')
    WHERE product_id IS NOT NULL
)
WHERE rn = 1;
""")

# Stats dedup
# dedup_stats = con.execute("""
# SELECT COUNT(*) AS total_dedup FROM link_dedup
# """).fetchone()
# removed = stats[1] - dedup_stats[0]

# print(f"Rows after dedup: {dedup_stats[0]}, duplicates removed: {removed}")

# Export dedup
con.execute(f"COPY link_dedup TO '{dedup_file}' (FORMAT PARQUET)")

print("✅ Pipeline link_local terminé.")