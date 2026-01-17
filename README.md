ONT THIS PROJECT I WILL GIVE 7 SECTION :
1. Project Overview
2. Architecture
3. Tech Stack
4. Data Pipeline Flow
5. How to Run (Local)
6. Analytics Examples
7. Key Learnings

# ETL Pipeline: Public API → PostgreSQL → Analytics Layer

##  Section 1: Project overview
This project show an **end-to-end Data Engineering pipeline** that ingests data from a public API(fake), transforms it into largescale analytical datasets, and loads it into PostgreSQL for business analytics.

The pipeline simulates **real-world data volume** (51110+ rows) by generating time-based price snapshots, making it suitable for analytics use cases such as trend analysis and price volatility reporting.

This project is designed to reflect **industry style ETL workflows**, not tutorial-level scripts.

---

## Section 2: Architecture

Public API  
→ Extract (Python)  
→ Transform (Data augmentation & validation)  
→ Raw Layer (PostgreSQL)  
→ Analytics Layer (Fact table & SQL queries)

**Key concept:** Raw data is separated from analytics-ready data to ensure scalability and correctness.

---

## Section 3: Tech stack(tools)

- Python 3
- PostgreSQL
- SQL
- Virtual Environment (venv)
- Git

---

## Section 4: Data pipeline flow

1. **Extract**
   - Fetch product data from Fake Store API
   - Validate schema and remove incomplete records

2. **Transform**
   - Generate 51,000 rows using:
     - Daily snapshots (365 days)
     - Multiple price variations per day
     - Apply data validation (price > 0)

3. **Load**
   - Batch insert into PostgreSQL
   - raw table design

4. **Analytics**
   - Aggregate raw data into a daily fact table
   - Enable business-level SQL analysis

---

## Section 5: How we can run this project locally

1. Create virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

2. Create database
mine :createdb etl_db

3. Run ETL pipeline
python -m etl.load (windows users)
python3 -m etl.load

---
## section 6: Analytics examples
Section 6i) Daily average price per category
SELECT 
    category,
    snapshot_date,
    ROUND(AVG(avg_price), 2) AS category_avg_price
FROM fact_daily_product_price
GROUP BY category, snapshot_date;


Section 6ii) Most price -unpredict products
SELECT
    title,
    ROUND(AVG(max_price - min_price), 2) AS avg_daily_price_range
FROM fact_daily_productprice
GROUP BY title
ORDER BY avg_daily_price_range
LIMIT 10;

Section 6iii) Highest average price products

SELECT
    title,
    ROUND(AVG(avg_pricce), 2) as overall_avg_price
    FROM fact_daily_product_price
    GROUP BY title
    ORDER BY overall_avg_price


Section 7 : Key learning 
1.)Designing raw vs analytics layers is critical for scalable data systems.

2.)Data augmentation can simulate realistic data volume for portfolio projects

3.)Batch inserts and idempotent loads prevent data duplication.

4.)Analytics tables should be optimized for query simplicity, not ingestion speed