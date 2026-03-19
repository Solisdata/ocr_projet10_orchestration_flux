'''Extraction de l’inventaire de l’ERP, avec des informations sur la disponibilité et le prix des produits pour alimenter le site web ou d’autres processus.
Chaque ligne correspond à un produit et chaque colonne décrit une caractéristique ou un état :
    product_id : identifiant unique du produit dans l’ERP.
    onsale_web : indique si le produit est vendu sur le site web (1 = oui, 0 = non).
    price : prix du produit (en € dans ce cas).
    stock_quantity : quantité disponible en stock.
    stock_status : état du stock, par exemple :
    outofstock = rupture de stock
    instock = en stock'''



import pandas as pd

erp_df = pd.read_excel("data/raw/Fichier_erp.xlsx")
erp_df.to_parquet("data/silver/erp.parquet", engine="fastparquet")


print("✅ Load ERP terminé, fichiers prêts pour les étapes suivantes")