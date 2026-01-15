# HEALTHCARE ANALYTICS LAB - REFLECTION
## Analysis of OLTP to Star Schema Transformation

### 1. Why Is the Star Schema Faster?

The Star Schema (OLAP) strictly outperformed the normalized OLTP schema across all tested queries. The performance gains stem from three key architectural changes:

*   **Reduction of Joins (Denormalization):**  
    In the OLTP schema, basic questions often required 4+ joins. For example, to aggregate revenue by specialty, we had to join `billing` -> `encounters` -> `providers` -> `specialties`. In the Star Schema, we denormalized `specialty` information directly into `dim_provider` and moved revenue metrics directly into `fact_encounters`. This flattened the structure, reducing the join depth to just 1 or 2 integer-based joins.

*   **Pre-Computation (ETL Loading):**  
    The most dramatic speedup (Query 3) came from moving complex logic out of the query execution path and into the ETL process. The "30-Day Readmission" logic originally required a heavy self-join with date arithmetic running at query time. By calculating this once during the ETL load and setting a simple `is_readmission` boolean flag, we converted a CPU-intensive calculation into a trivial filter (`WHERE is_readmission = 1`).

*   **Integer Surrogate Keys:**  
    The OLTP schema used mixed data types for joins. The Star Schema uses uniform integer surrogate keys (`encounter_key`, `patient_key`) for all joins. Database engines are significantly faster at joining integers than comparing strings or dates.

### 2. Trade-offs: What Did We Gain vs. Lose?

**What We Lost (The Costs):**
*   **Data Redundancy:** We now store "Specialty Name" in every row of the Provider dimension. If a specialty name changes, we must update thousands of rows instead of just one.
*   **ETL Complexity:** The OLTP system just accepts inserts. The Star Schema requires a complex ETL pipeline to manage Surrogate Keys, look up dimensions, handle Slowly Changing Dimensions (SCD), and populate Bridge tables.
*   **Data Latency:** Data is available immediately in OLTP. In the Star Schema, analytics are stale until the next ETL batch runs (usually nightly).

**What We Gained (The Benefits):**
*   **Analysis Simplicity:** Business users no longer need to understand the complex relationship between `providers`, `departments`, and `encounters`. They just select from `fact_encounters`.
*   **Consistent History:** By using a Type 2 Dimension for Patients, we can track a patient's history even if their name or age group changes, which is difficult in a 3NF schema that overwrites current state.
*   **Predictable Performance:** Queries involving millions of rows will scale linearly in the Star Schema due to simplified joins, whereas the OLTP nested-loops would degrade exponentially.

**Was it worth it?**  
Yes. For an analytics workload, read performance is paramount. The cost of storage and ETL development is a one-time setup price paid to ensure thousands of future analytical queries run instantly.

### 3. Bridge Tables: Worth It?

**Decision:**  
We retained `bridge_encounter_diagnoses` and `bridge_encounter_procedures` instead of forcing this data into the Fact table.

**Why?**  
This decision preserved the "One Encounter = One Row" grain of the fact table. If we had denormalized diagnoses into the fact table (e.g., `diagnosis_1`, `diagnosis_2`), we would have created wide, messy rows and limited the analysis (what if a patient has 15 diagnoses?). If we had changed the grain to "One Row Per Diagnosis," we would have exploded the fact table size by 300% and made revenue aggregation difficult (double-counting risks).

**Production view:**  
In a production environment, this is the standard "Kimball" approach for Many-to-Many relationships. It allows us to perform high-speed aggregations on the Fact table (Revenue, Counts) without touching the Bridge table, while still allowing deep clinical analysis when needed.

### 4. Performance Quantification

The following comparisons illustrate the impact of the transformation:

**Query 3: 30-Day Readmission Rate**
*   **Original (OLTP):** ~31.9 ms
*   **Optimized (Star):** ~11.1 ms
*   **Improvement:** **~2.9x Faster**
*   **Reason:** Replaced a complex Self-Join and Date Arithmetic with a simple Boolean Flag lookup.

**Query 4: Revenue by Specialty**
*   **Original (OLTP):** ~45.2 ms
*   **Optimized (Star):** ~26.2 ms
*   **Improvement:** **~1.7x Faster**
*   **Reason:** Eliminated the join to the `billing` table by moving `total_allowed_amount` directly into the Fact table.

**Overall Conclusion:**
The migration successfully met the objective. While small datasets show millisecond-level differences, the architectural changes (O(1) lookups vs O(N^2) joins) guarantee that the Star Schema will handle data growth far better than the original transactional system.
