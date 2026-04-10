import pandas as pd
from sqlalchemy import create_engine, text

# ── Connexion ────────────────────────────────────────────────────────────────
engine = create_engine(
    "postgresql+psycopg2://etl_user:etl_pass@localhost:5432/retail_db"
)

# ── Fonction utilitaire ───────────────────────────────────────────────────────
def load(query):
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)

# ── Table 1 : Transactions nettoyées ─────────────────────────────────────────
retail_transactions = load("""
    SELECT
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "TotalAmount",
        "CustomerID",
        "Country",
        EXTRACT(YEAR  FROM "InvoiceDate")::int  AS year,
        EXTRACT(MONTH FROM "InvoiceDate")::int  AS month,
        EXTRACT(DOW   FROM "InvoiceDate")::int  AS day_of_week,
        EXTRACT(HOUR  FROM "InvoiceDate")::int  AS hour_of_day
    FROM retail_transactions
    WHERE "TotalAmount" > 0
    LIMIT 100000
""")

# ── Table 2 : KPIs globaux (dernier batch uniquement) ────────────────────────
kpis_global = load("""
    SELECT
        SUM(total_revenue)       AS total_revenue,
        SUM(total_transactions)  AS total_transactions,
        SUM(unique_customers)    AS unique_customers,
        MAX(active_countries)    AS active_countries,
        MAX(unique_products)     AS unique_products,
        AVG(avg_basket_value)    AS avg_basket_value,
        MAX(max_order_value)     AS max_order_value,
        SUM(total_units_sold)    AS total_units_sold,
        AVG(revenue_per_customer) AS revenue_per_customer
    FROM kpis_global
""")

# ── Table 3 : Ventes par pays (dédupliqué) ────────────────────────────────────
sales_by_country = load("""
    SELECT
        "Country",
        SUM(total_revenue)      AS total_revenue,
        SUM(invoice_count)      AS invoice_count,
        AVG(avg_order_value)    AS avg_order_value,
        SUM(unique_customers)   AS unique_customers,
        SUM(total_units_sold)   AS total_units_sold
    FROM sales_by_country
    GROUP BY "Country"
    ORDER BY total_revenue DESC
""")

# ── Table 4 : Ventes par produit (dédupliqué) ────────────────────────────────
sales_by_product = load("""
    SELECT
        "StockCode",
        "Description",
        SUM(total_qty_sold)   AS total_qty_sold,
        SUM(total_revenue)    AS total_revenue,
        AVG(avg_unit_price)   AS avg_unit_price,
        SUM(unique_buyers)    AS unique_buyers,
        MAX(countries_reached) AS countries_reached
    FROM sales_by_product
    GROUP BY "StockCode", "Description"
    ORDER BY total_revenue DESC
    LIMIT 200
""")

# ── Table 5 : Ventes par mois ─────────────────────────────────────────────────
sales_by_month = load("""
    SELECT
        year,
        month,
        TO_DATE(year::text || '-' || LPAD(month::text,2,'0') || '-01', 'YYYY-MM-DD')
            AS date_mois,
        SUM(total_revenue)    AS total_revenue,
        SUM(invoice_count)    AS invoice_count,
        SUM(unique_customers) AS unique_customers,
        AVG(avg_basket_value) AS avg_basket_value
    FROM sales_by_month
    GROUP BY year, month
    ORDER BY year, month
""")

# ── Table 6 : Ventes par heure ────────────────────────────────────────────────
sales_by_hour = load("""
    SELECT
        hour_of_day,
        SUM(total_revenue)      AS total_revenue,
        SUM(transaction_count)  AS transaction_count,
        AVG(avg_order_value)    AS avg_order_value
    FROM sales_by_hour
    GROUP BY hour_of_day
    ORDER BY hour_of_day
""")

# ── Table 7 : Ventes par jour de la semaine ───────────────────────────────────
sales_by_dayofweek = load("""
    SELECT
        CASE day_of_week
            WHEN 1 THEN 'Dimanche'
            WHEN 2 THEN 'Lundi'
            WHEN 3 THEN 'Mardi'
            WHEN 4 THEN 'Mercredi'
            WHEN 5 THEN 'Jeudi'
            WHEN 6 THEN 'Vendredi'
            WHEN 7 THEN 'Samedi'
        END AS jour,
        day_of_week,
        SUM(total_revenue)      AS total_revenue,
        SUM(transaction_count)  AS transaction_count,
        AVG(avg_order_value)    AS avg_order_value
    FROM sales_by_dayofweek
    GROUP BY day_of_week
    ORDER BY day_of_week
""")

# ── Table 8 : Stats clients (dédupliqué) ──────────────────────────────────────
customer_stats = load("""
    SELECT
        "CustomerID",
        "Country",
        SUM(total_spent)        AS total_spent,
        SUM(total_orders)       AS total_orders,
        AVG(avg_order_value)    AS avg_order_value,
        SUM(total_units_bought) AS total_units_bought,
        MAX(last_purchase_date) AS last_purchase_date
    FROM customer_stats
    WHERE "CustomerID" != 'Unknown'
    GROUP BY "CustomerID", "Country"
    ORDER BY total_spent DESC
    LIMIT 500
""")

print("Toutes les tables chargées avec succès")