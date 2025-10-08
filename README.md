# 🚦 Traffic Accidents in Mexico — Data Engineering Project (2022–2023)

### 💾 End-to-End Data Pipeline, Database Design, and Analytical Insights

---

## 📁 Project Overview

This project is an **end-to-end data engineering pipeline** designed to collect, clean, process, and analyze traffic accident data in Mexico for the **years 1997 and 2023**.

Due to infrastructure limitations, we focused on the analysis of the **two most recent years**, which still offer valuable insights into current traffic safety trends. The pipeline and database use the entire data.

🔎 **Key objectives:**

- Build a robust **ETL pipeline** to ingest and transform raw accident data.   
- Design a **relational database schema** optimized for analytics and reporting.  
- Deliver a clean, query-ready dataset for dashboards and machine learning use cases.  
- Provide data-driven **insights and KPIs** for public safety and mobility planning.

---

## 🛠️ Data Engineering Architecture

The project follows a modular data pipeline design:

```text
📥 Ingestion   →   🧹 Transformation   →   💾 Storage   →   📊 Analysis & Visualization
CSV/INEGI         Cleaning, validation    SQLite DB        Dashboards & KPIs
```

### 🔄 Pipeline Steps

1. **Extract:**  
   - Load raw CSV files (2022 & 2023) from official traffic accident data sources.

2. **Transform:**  
   - Standardize data types (dates, numeric fields, categories).  
   - Clean and handle null values and inconsistencies.  
   - Enrich dataset with new columns (e.g., month, weekday, accident category).  
   - Normalize categorical data into separate tables.

3. **Load:**  
   - Store processed data in a **SQLite relational database**.  
   - Create **foreign key relationships** (e.g., `estado`, `municipio`) for better querying performance.

4. **Analyze:**  
   - Run queries and generate aggregated KPIs.  
   - Build visual dashboards to highlight key trends.

---

## 🗃️ Database Schema

The database is normalized into a **star-schema-like structure** optimized for analytical queries:

```
📊 accidentes (fact table)
├── id (PK)
├── municipio_id (FK)
├── tipo_accidente
├── causa_accidente
├── superficie_rodamiento
├── sexo
├── aliento_alcoholico
├── cinturon
├── edad
├── fecha_hora
├── zona
├── vehiculos_id (FK)
├── heridos_id (FK)
└── muertos_id (FK)

📁 heridos (dimension)
├── id (PK)
├── conductor_herido
├── pasajero_herido
├── peaton_herido
├── ciclista_herido
├── otro_herido
└── no_espec_herido

📁 muertos (dimension)
├── id (PK)
├── conductor_muerto
├── pasajero_muerto
├── peaton_muerto
├── ciclista_muerto
├── otro_muerto
└── no_espec_muerto

📁 vehiculos (dimension)
├── id (PK)
├── automovil
├── camioneta_pasajeros
├── microbus
├── camion_urbano
├── autobus
├── tranvia
├── camioneta_carga
├── camion_carga
├── tractor
├── ferrocarril
├── motocicleta
├── bicicleta
└── otro_vehiculo

📁 entidades (dimension)
├── id (PK)
└── nombre

📁 municipios (dimension)
├── id (PK)
├── nombre
└── entidad_id (PK)
```

---