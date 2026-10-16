# =====================================================================
# Notebook Fabric : NB_bronze_to_silver
# Lakehouse par défaut : LH_DataShop
# Lit les fichiers bruts de Files/bronze, nettoie, écrit les tables Silver (Delta)
# Chaque bloc "# %% CELLULE n" = une cellule du notebook
# =====================================================================

# %% CELLULE 1 - Imports et paramètres
from pyspark.sql import functions as F

BRONZE = "Files/bronze"          # dossier des fichiers bruts dans le Lakehouse

def clean_city(col):
    """'  BOULOGNE-BILLANCOURT ' => 'Boulogne-Billancourt' (initcap seul ne gère pas le tiret)."""
    c = F.lower(F.trim(col))
    return F.regexp_replace(F.initcap(F.regexp_replace(c, "-", "- ")), "- ", "-")

def save_silver(df, name):
    """Écrit un DataFrame en table Delta Silver (écrasement complet)."""
    (df.write.mode("overwrite")
       .option("overwriteSchema", "true")
       .format("delta")
       .saveAsTable(name))
    print(f"✅ {name} : {spark.table(name).count():,} lignes")


# %% CELLULE 2 - Lecture des ventes brutes (3 fichiers d'un coup)
sales_raw = (spark.read
    .option("header", True)
    .option("sep", ";")                 # export caisse français => point-virgule
    .option("inferSchema", False)       # en Bronze on lit TOUT en texte
    .csv(f"{BRONZE}/sales_*.csv"))

print(sales_raw.count(), "lignes brutes")
display(sales_raw.limit(10))


# %% CELLULE 3 - Diagnostic qualité (à montrer à l'écran)
# 3a. Formats de date
display(sales_raw
    .withColumn("format_date", F.when(F.col("order_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), "aaaa-mm-jj")
                                 .when(F.col("order_date").rlike(r"^\d{2}/\d{2}/\d{4}$"), "jj/mm/aaaa")
                                 .otherwise("autre"))
    .groupBy("format_date").count())

# 3b. Valeurs de statut
display(sales_raw.groupBy("status").count().orderBy(F.desc("count")))

# 3c. Doublons exacts
print("Doublons :", sales_raw.count() - sales_raw.dropDuplicates().count())


# %% CELLULE 4 - Nettoyage des ventes
status_clean = F.lower(F.trim(F.col("status")))
status_clean = F.translate(status_clean, "éè", "ee")

sales = (sales_raw
    .dropDuplicates()
    # dates : on essaie les deux formats
    .withColumn("order_date", F.when(F.col("order_date").rlike(r"^\d{2}/\d{2}/\d{4}$"),
                                     F.to_date("order_date", "dd/MM/yyyy"))
                               .otherwise(F.to_date("order_date", "yyyy-MM-dd")))
    # prix : virgule => point, puis décimal
    .withColumn("unit_price", F.regexp_replace("unit_price", ",", ".").cast("decimal(10,2)"))
    .withColumn("discount_pct", F.col("discount_pct").cast("decimal(4,2)"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("order_line", F.col("order_line").cast("int"))
    # identifiants
    .withColumn("product_id", F.upper(F.trim("product_id")))
    .withColumn("customer_id", F.when(F.trim("customer_id") == "", None).otherwise(F.trim("customer_id")))
    # statut harmonisé
    .withColumn("status", F.when(status_clean == "livree", "Livrée")
                           .when(status_clean == "retournee", "Retournée")
                           .when(status_clean == "annulee", "Annulée")
                           .otherwise("Inconnu"))
    # règles métier
    .filter(F.col("quantity") > 0)
    # montants
    .withColumn("gross_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"))
    .withColumn("discount_amount", F.round(F.col("gross_amount") * F.col("discount_pct"), 2).cast("decimal(12,2)"))
    .withColumn("net_amount", (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(12,2)"))
    .withColumn("_loaded_at", F.current_timestamp())
)

# contrôles : aucune date ni prix nul, aucun statut inconnu
display(sales.select(
    F.sum(F.col("order_date").isNull().cast("int")).alias("dates_nulles"),
    F.sum(F.col("unit_price").isNull().cast("int")).alias("prix_nuls"),
    F.sum((F.col("status") == "Inconnu").cast("int")).alias("statuts_inconnus")))

save_silver(sales, "silver_sales")


# %% CELLULE 5 - Clients
customers_raw = spark.read.option("header", True).csv(f"{BRONZE}/customers.csv")

customers = (customers_raw
    .dropDuplicates()
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("city", clean_city("city"))
    .withColumn("postal_code", F.lpad("postal_code", 5, "0"))
    .withColumn("birth_date", F.to_date("birth_date"))
    .withColumn("signup_date", F.to_date("signup_date"))
    .withColumn("_loaded_at", F.current_timestamp())
)

# contrôle : 1 ligne par client
assert customers.count() == customers.select("customer_id").distinct().count(), "Doublon de customer_id !"
save_silver(customers, "silver_customers")


# %% CELLULE 6 - Produits (JSON imbriqué => colonnes à plat)
products_raw = spark.read.option("multiline", True).json(f"{BRONZE}/products.json")
products_raw.printSchema()

products = products_raw.select(
    "product_id",
    "product_name",
    "brand",
    F.col("category.name").alias("category"),
    F.col("category.subcategory").alias("subcategory"),
    F.col("pricing.list_price").cast("decimal(10,2)").alias("list_price"),
    F.col("pricing.unit_cost").cast("decimal(10,2)").alias("unit_cost"),
    F.to_date("launch_date").alias("launch_date"),
    "is_active",
).withColumn("_loaded_at", F.current_timestamp())

save_silver(products, "silver_products")


# %% CELLULE 7 - Magasins
stores = (spark.read.option("header", True).csv(f"{BRONZE}/stores.csv")
    .withColumn("city", clean_city("city"))
    .withColumn("opening_date", F.to_date("opening_date"))
    .withColumn("surface_m2", F.col("surface_m2").cast("int"))
    .withColumn("_loaded_at", F.current_timestamp()))

save_silver(stores, "silver_stores")


# %% CELLULE 8 - Table calendrier (générée, pas ingérée)
mois = F.array(*[F.lit(m) for m in ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet",
                                    "Août", "Septembre", "Octobre", "Novembre", "Décembre"]])
jours = F.array(*[F.lit(j) for j in ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]])

dates = (spark.sql("SELECT explode(sequence(to_date('2024-01-01'), to_date('2026-12-31'), interval 1 day)) AS date")
    .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.concat(F.lit("T"), F.quarter("date")))
    .withColumn("month_num", F.month("date"))
    .withColumn("month_name", F.element_at(mois, F.month("date")))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    .withColumn("day_of_week_num", F.dayofweek("date") - 1)          # 0 = dimanche
    .withColumn("day_of_week_num", F.when(F.col("day_of_week_num") == 0, 7).otherwise(F.col("day_of_week_num")))
    .withColumn("day_name", F.element_at(jours, F.col("day_of_week_num")))
    .withColumn("is_weekend", F.col("day_of_week_num") >= 6)
    .withColumn("month_start", F.trunc("date", "month")))

save_silver(dates, "silver_date")


# %% CELLULE 9 - Récap
for t in ["silver_sales", "silver_customers", "silver_products", "silver_stores", "silver_date"]:
    print(f"{t:<20} {spark.table(t).count():>10,}")
