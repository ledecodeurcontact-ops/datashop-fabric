/* =====================================================================
   Warehouse Fabric : WH_DataShop  —  couche GOLD (modèle en étoile)
   Source : tables Silver du Lakehouse LH_DataShop (même workspace)
            => lues en "requête inter-bases" : LH_DataShop.dbo.<table>
   Exécution : coller dans un nouvel éditeur SQL du Warehouse, tout exécuter.
   Ensuite : EXEC dbo.sp_load_gold;   (appelé aussi par le pipeline)
   ===================================================================== */

-- ---------------------------------------------------------------------
-- ÉTAPE 1 (à montrer) : on peut requêter le Lakehouse depuis le Warehouse
-- ---------------------------------------------------------------------
SELECT TOP 10 * FROM LH_DataShop.dbo.silver_sales;

SELECT status, COUNT(*) AS nb_lignes, SUM(net_amount) AS ca_net
FROM LH_DataShop.dbo.silver_sales
GROUP BY status;
GO

-- ---------------------------------------------------------------------
-- ÉTAPE 2 : la procédure qui (re)construit toute la couche Gold
-- ---------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.sp_load_gold
AS
BEGIN

    /* ============ DIMENSION DATE ============ */
    DROP TABLE IF EXISTS dbo.dim_date;
    CREATE TABLE dbo.dim_date AS
    SELECT
        CAST(date_key        AS INT)         AS date_key,
        CAST([date]          AS DATE)        AS [date],
        CAST([year]          AS INT)         AS [year],
        CAST(quarter         AS VARCHAR(2))  AS quarter,
        CAST(month_num       AS INT)         AS month_num,
        CAST(month_name      AS VARCHAR(12)) AS month_name,
        CAST(year_month      AS VARCHAR(7))  AS year_month,
        CAST(day_of_week_num AS INT)         AS day_of_week_num,
        CAST(day_name        AS VARCHAR(10)) AS day_name,
        CAST(is_weekend      AS BIT)         AS is_weekend,
        CAST(month_start     AS DATE)        AS month_start
    FROM LH_DataShop.dbo.silver_date;

    /* ============ DIMENSION MAGASIN ============ */
    DROP TABLE IF EXISTS dbo.dim_store;
    CREATE TABLE dbo.dim_store AS
    SELECT
        CAST(store_id   AS VARCHAR(5))   AS store_id,
        CAST(store_name AS VARCHAR(60))  AS store_name,
        CAST(city       AS VARCHAR(50))  AS city,
        CAST(region     AS VARCHAR(50))  AS region,
        CAST(store_type AS VARCHAR(10))  AS channel,          -- Magasin / Web
        CAST(opening_date AS DATE)       AS opening_date,
        CAST(surface_m2 AS INT)          AS surface_m2
    FROM LH_DataShop.dbo.silver_stores;

    /* ============ DIMENSION PRODUIT ============ */
    DROP TABLE IF EXISTS dbo.dim_product;
    CREATE TABLE dbo.dim_product AS
    SELECT
        CAST(product_id   AS VARCHAR(5))   AS product_id,
        CAST(product_name AS VARCHAR(60))  AS product_name,
        CAST(brand        AS VARCHAR(30))  AS brand,
        CAST(category     AS VARCHAR(30))  AS category,
        CAST(subcategory  AS VARCHAR(40))  AS subcategory,
        CAST(list_price   AS DECIMAL(10,2)) AS list_price,
        CAST(unit_cost    AS DECIMAL(10,2)) AS unit_cost,
        CAST(CASE
                WHEN list_price < 100  THEN '1. Moins de 100 €'
                WHEN list_price < 500  THEN '2. 100 à 499 €'
                WHEN list_price < 1000 THEN '3. 500 à 999 €'
                ELSE '4. 1 000 € et plus'
             END AS VARCHAR(20))            AS price_range,
        CAST(is_active    AS BIT)           AS is_active
    FROM LH_DataShop.dbo.silver_products;

    /* ============ DIMENSION CLIENT ============ */
    -- + une ligne "Client non identifié" pour les ventes magasin sans carte de fidélité
    DROP TABLE IF EXISTS dbo.dim_customer;
    CREATE TABLE dbo.dim_customer AS
    SELECT
        CAST(customer_id AS VARCHAR(6))                   AS customer_id,
        CAST(CONCAT(first_name, ' ', last_name) AS VARCHAR(80)) AS customer_name,
        CAST(gender      AS VARCHAR(1))                   AS gender,
        CAST(city        AS VARCHAR(50))                  AS city,
        CAST(postal_code AS VARCHAR(5))                   AS postal_code,
        CAST(LEFT(postal_code, 2) AS VARCHAR(2))          AS department,
        CAST(loyalty_tier AS VARCHAR(10))                 AS loyalty_tier,
        CAST(signup_date AS DATE)                         AS signup_date,
        CAST(CASE
                WHEN birth_date IS NULL THEN 'Inconnu'
                WHEN DATEDIFF(YEAR, birth_date, '2026-09-30') < 25 THEN '18-24 ans'
                WHEN DATEDIFF(YEAR, birth_date, '2026-09-30') < 35 THEN '25-34 ans'
                WHEN DATEDIFF(YEAR, birth_date, '2026-09-30') < 50 THEN '35-49 ans'
                WHEN DATEDIFF(YEAR, birth_date, '2026-09-30') < 65 THEN '50-64 ans'
                ELSE '65 ans et +'
             END AS VARCHAR(12))                          AS age_band
    FROM LH_DataShop.dbo.silver_customers
    UNION ALL
    SELECT 'C00000', 'Client non identifié', NULL, NULL, NULL, NULL, 'Aucun', NULL, 'Inconnu';

    /* ============ FAIT VENTES (grain = 1 ligne de commande) ============ */
    DROP TABLE IF EXISTS dbo.fact_sales;
    CREATE TABLE dbo.fact_sales AS
    SELECT
        CAST(s.order_id   AS VARCHAR(12))                         AS order_id,
        CAST(s.order_line AS INT)                                 AS order_line,
        CAST(YEAR(s.order_date) * 10000 + MONTH(s.order_date) * 100 + DAY(s.order_date) AS INT) AS date_key,
        CAST(COALESCE(s.customer_id, 'C00000') AS VARCHAR(6))     AS customer_id,
        CAST(s.store_id   AS VARCHAR(5))                          AS store_id,
        CAST(s.product_id AS VARCHAR(5))                          AS product_id,
        CAST(s.payment_method AS VARCHAR(20))                     AS payment_method,
        CAST(s.status     AS VARCHAR(10))                         AS status,
        CAST(s.quantity   AS INT)                                 AS quantity,
        CAST(s.unit_price AS DECIMAL(10,2))                       AS unit_price,
        CAST(s.gross_amount    AS DECIMAL(12,2))                  AS gross_amount,
        CAST(s.discount_amount AS DECIMAL(12,2))                  AS discount_amount,
        -- CA réellement encaissé : 0 si la ligne est annulée ou retournée
        CAST(CASE WHEN s.status = 'Livrée' THEN s.net_amount ELSE 0 END AS DECIMAL(12,2)) AS net_amount,
        CAST(CASE WHEN s.status = 'Livrée' THEN s.quantity * p.unit_cost ELSE 0 END AS DECIMAL(12,2)) AS cost_amount,
        CAST(CASE WHEN s.status = 'Retournée' THEN 1 ELSE 0 END AS INT) AS is_returned,
        CAST(CASE WHEN s.status = 'Annulée'   THEN 1 ELSE 0 END AS INT) AS is_cancelled
    FROM LH_DataShop.dbo.silver_sales AS s
    LEFT JOIN LH_DataShop.dbo.silver_products AS p
           ON p.product_id = s.product_id;

    /* ============ FAIT OBJECTIFS (grain = 1 magasin x 1 mois) ============ */
    -- grain différent de fact_sales : on rattache l'objectif au 1er jour du mois
    DROP TABLE IF EXISTS dbo.fact_targets;
    CREATE TABLE dbo.fact_targets AS
    SELECT
        CAST(store_id AS VARCHAR(5))                                   AS store_id,
        CAST([year] * 10000 + [month] * 100 + 1 AS INT)                AS date_key,
        CAST(target_amount AS DECIMAL(12,2))                           AS target_amount
    FROM LH_DataShop.dbo.silver_targets;

END;
GO

-- ---------------------------------------------------------------------
-- ÉTAPE 3 : on lance et on contrôle
-- ---------------------------------------------------------------------
EXEC dbo.sp_load_gold;

-- volumétrie
SELECT 'dim_date' AS table_name, COUNT(*) AS nb FROM dbo.dim_date
UNION ALL SELECT 'dim_store',    COUNT(*) FROM dbo.dim_store
UNION ALL SELECT 'dim_product',  COUNT(*) FROM dbo.dim_product
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dbo.dim_customer
UNION ALL SELECT 'fact_sales',   COUNT(*) FROM dbo.fact_sales
UNION ALL SELECT 'fact_targets', COUNT(*) FROM dbo.fact_targets;

-- intégrité : chaque vente retrouve son client, son produit, son magasin, sa date (doit renvoyer 0 partout)
SELECT
    SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS sans_client,
    SUM(CASE WHEN p.product_id  IS NULL THEN 1 ELSE 0 END) AS sans_produit,
    SUM(CASE WHEN st.store_id   IS NULL THEN 1 ELSE 0 END) AS sans_magasin,
    SUM(CASE WHEN d.date_key    IS NULL THEN 1 ELSE 0 END) AS sans_date
FROM dbo.fact_sales f
LEFT JOIN dbo.dim_customer c ON c.customer_id = f.customer_id
LEFT JOIN dbo.dim_product  p ON p.product_id  = f.product_id
LEFT JOIN dbo.dim_store   st ON st.store_id   = f.store_id
LEFT JOIN dbo.dim_date     d ON d.date_key    = f.date_key;

-- première analyse : CA net par année et canal
SELECT d.[year], st.channel, SUM(f.net_amount) AS ca_net
FROM dbo.fact_sales f
JOIN dbo.dim_date  d  ON d.date_key  = f.date_key
JOIN dbo.dim_store st ON st.store_id = f.store_id
GROUP BY d.[year], st.channel
ORDER BY d.[year], st.channel;
