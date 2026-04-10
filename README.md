# etlproject
projet etl 
# 🚀 Retail ETL Pipeline — Kafka + Spark + Power BI

Pipeline ETL streaming end-to-end entièrement dockerisé,
traitant des données retail en temps réel.


## Stack technique
- **Apache Kafka** — message broker, découplage source/traitement
- **Apache Spark Structured Streaming** — transformations + agrégations
- **PostgreSQL** — stockage des données transformées (8 tables)
- **Power BI** — dashboards et KPIs
- **Docker Compose** — orchestration de tous les services

## Dataset
UCI Online Retail Dataset — 500 000+ transactions retail
(InvoiceNo, StockCode, Description, Quantity, UnitPrice, CustomerID, Country)

## Lancement rapide

```bash
git clone https://github.com/anasssearrachi/etlproject
cd etl-retail-kafka-spark-powerbi
cp .env.example .env
# Ajouter le chemin de ton CSV dans .env
docker-compose up --build -d
```

## Ce que fait le pipeline

1. **Extract** — Producer Python lit le CSV ligne par ligne → publie dans Kafka
2. **Transform** — Spark nettoie, caste, agrège en micro-batch toutes les 10s
3. **Load** — 8 tables écrites dans PostgreSQL

## Transformations Spark
- Suppression des doublons
- Filtre des valeurs nulles et négatives
- Exclusion des retours clients (InvoiceNo commençant par C)
- Calcul TotalAmount = Quantity × UnitPrice
- Agrégations : CA par pays, top produits, KPIs globaux, stats horaires


