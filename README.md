# Healthcare Analytics Lab: OLTP to Star Schema

## 🏥 Overview
This project simulates a real-world Data Engineering task at "HealthTech Analytics". The clinical team originally built a normalized transactional database (3NF) which struggled with slow analytical queries. 

**My Role:** As the Data Engineer, I analyzed the performance bottlenecks, designed a Kimball-style Star Schema, implemented the ETL logic, and verified significant performance improvements (up to 3x faster) for key business metrics.

---

## 📂 Repository Contents

### 1. Interactive Analysis 📓
*   **`notebooks/healthcare_analytics_exploration.ipynb`**: This is the core of the project. It contains:
    *   Setup code to initialize the OLTP database.
    *   **Performance Analysis (OLTP)**: Execution of business queries against the normalized schema with `EXPLAIN ANALYZE` metrics.
    *   **Star Schema Implementation**: DDL execution and ETL data loading.
    *   **Performance Comparison (OLAP)**: Side-by-side benchmarking showing the speed improvements of the new design.

### 2. Deliverables & Documentation 📄
All required project artifacts are located in the `deliverables/` folder:
*   **`star_schema.sql`**: The complete definition of the optimized database (Dimensions, Facts, Bridges) and the ETL scripts to populate them.
*   **`star_schema_queries.txt`**: The optimized SQL queries for the 4 business questions.
*   **`design_decisions.txt`**: Detailed justification for grain choices, dimension handling, and bridge tables.
*   **`etl_design.txt`**: The strategy document for loading data, handling SCDs, and pre-aggregating metrics.
*   **`reflection.md`**: A final report analyzing "Why" the Star Schema is faster and the trade-offs involved.

### 3. Data Scripts 💾
*   `data/generated_10k_sample_data.sql`: The source script used to seed the initial OLTP database environment.

---

## 🚀 Key Improvements Achieved

| Business Metric | OLTP Strategy | Star Schema Strategy | Improvement |
| :--- | :--- | :--- | :--- |
| **30-Day Readmissions** | Complex Self-Join + Date Math | Pre-computed `is_readmission` flag | **~2.9x Faster** |
| **Revenue by Specialty** | 4-Table Join Chain | Denormalized Fact Table | **~1.7x Faster** |
| **Monthly Encounters** | Runtime `DATE_FORMAT()` | Pre-computed Date Dimension | **~6% Faster** |

---

## 🛠️ How to Run

1.  **Prerequisites**: Ensure you have Python and a MySQL compatible environment (local or Docker) running, plus the required Python packages (for example: `jupyter`/`notebook`, `pandas`, and a MySQL connector such as `mysql-connector-python` or `pymysql`). If a `requirements.txt` file is provided, install dependencies with `pip install -r requirements.txt`.
2.  **Explore the Notebook**: Open `notebooks/healthcare_analytics_exploration.ipynb` in VS Code.
3.  **Execute sequentially**:
    *   Run the "Part 1" cells to build the OLTP environment.
    *   Run "Part 2" to see the slow query performance.
    *   Run "Part 3" to build the Star Schema and populate it.
    *   Run "Part 4" to witness the performance gains.

---

## ✅ Project Checklist
- [x] Analyze OLTP Performance bottlenecks
- [x] Design Star Schema (Fact, Dimensions, Bridges)
- [x] Implement DDL & ETL Scripts
- [x] Optimize Business Queries
- [x] Document Design Decisions & ETL Strategy
- [x] Final Reflection & Performance Report
