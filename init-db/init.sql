CREATE TABLE IF NOT EXISTS retail_transactions (
    id            SERIAL PRIMARY KEY,
    "InvoiceNo"   VARCHAR(20),
    "StockCode"   VARCHAR(20),
    "Description" TEXT,
    "Quantity"    INTEGER,
    "InvoiceDate" TIMESTAMP,
    "UnitPrice"   NUMERIC(10,2),
    "TotalAmount" NUMERIC(10,2),
    "CustomerID"  VARCHAR(20),
    "Country"     VARCHAR(60),
    inserted_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sales_by_country (
    id              SERIAL PRIMARY KEY,
    "Country"       VARCHAR(60),
    total_revenue   NUMERIC(14,2),
    invoice_count   INTEGER,
    avg_order_value NUMERIC(10,2),
    inserted_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sales_by_product (
    id              SERIAL PRIMARY KEY,
    "StockCode"     VARCHAR(20),
    "Description"   TEXT,
    total_qty_sold  INTEGER,
    total_revenue   NUMERIC(14,2),
    inserted_at     TIMESTAMP DEFAULT NOW()
);