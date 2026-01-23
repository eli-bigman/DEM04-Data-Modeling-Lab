# 🔍 **SENIOR DATA ENGINEER CODE REVIEW PROMPT**
## Healthcare Analytics Lab: OLTP to Star Schema Transformation

---

## **PROJECT CONTEXT**
You are a **Senior Data Engineer** at a FinTech/HealthTech company reviewing a junior engineer's star schema implementation for a production-grade healthcare analytics data warehouse. The engineer has transformed a normalized OLTP database (3NF) into a Kimball-style dimensional model to improve analytical query performance.

This is **NOT** an academic exercise. You are evaluating this codebase as if it will be:
- Deployed to production within 2 weeks
- Used by 50+ business analysts running daily reports
- Processing 500K+ new encounters monthly
- Subject to HIPAA compliance audits
- Maintained by a 5-person data engineering team for the next 3+ years

---

## **REVIEW SCOPE**

Conduct a thorough code review across the following dimensions:

### **1. ARCHITECTURE & DESIGN DECISIONS**

**Dimensional Modeling:**
- ✅ Is the fact grain (one row per encounter) appropriate for ALL 4 business questions?
- ✅ Are bridge tables (`bridge_encounter_diagnoses`, `bridge_encounter_procedures`) the right pattern, or should this be a factless fact table?
- ⚠️ Why use SCD Type 2 for `dim_patient` and `dim_provider` but NOT for `dim_specialty` or `dim_department`? Is this consistent with real-world requirements?
- ⚠️ Are there missing dimensions? (e.g., `dim_diagnosis`, `dim_procedure` exist but aren't FK'd to fact table—why?)
- 🔴 What happens when a patient changes their name (marriage)? Does SCD Type 2 create duplicate historical reporting issues?

**Trade-off Analysis:**
- Did the engineer properly justify the denormalization of specialty/department into `dim_provider`?
- Is the 2.9x performance gain for readmissions worth the ETL complexity of pre-computing flags?
- What's the storage overhead? (Estimate based on 10K → 1M encounters)

---

### **2. SQL CODE QUALITY**

**ETL Script (star_schema.sql):**
- 🔴 **CRITICAL:** The ETL uses `TRUNCATE` and full reload. How does this handle:
  - Failures mid-ETL? (Partial deletes with no data?)
  - Concurrent analyst queries during ETL window?
  - Referential integrity during dimension loads?
- ⚠️ The readmission flag is updated **AFTER** fact table insert via a self-join. Is this transactionally safe? What if the update fails?
- ✅ Are surrogate key lookups using `JOIN ... ON natural_key AND is_current = TRUE` correct?
- ⚠️ Date dimension is generated via recursive CTE for 2024 only. What happens on Jan 1, 2025?
- 🔴 No error handling, logging, or audit trails. How do you debug when ETL produces wrong results?

**Query Performance (star_schema_queries.txt):**
- Are the indexes actually being used? (Run `EXPLAIN` to verify)
- The revenue query joins fact → provider → specialty. Is this a regression? (Denormalized specialty should be in fact table)
- Bridge table queries use `COUNT(DISTINCT encounter_key)`. Is there a risk of Cartesian product explosion at scale?

**Data Types & Constraints:**
- `DECIMAL(12,2)` for billing amounts—sufficient for $999M claims? Real hospitals have larger claims.
- `VARCHAR(100)` for names—will this handle international characters (UTF-8)?
- No `NOT NULL` constraints on fact table foreign keys. Can this cause orphaned records?
- No `CHECK` constraints (e.g., `discharge_date >= encounter_date`). Data quality risk?

---

### **3. ETL DESIGN & DATA PIPELINE**

**Incremental Loading (etl_design.txt):**
- 🔴 **SHOWSTOPPER:** Current design is full refresh. The doc mentions "In production, this would be incremental" but provides NO implementation. 
  - How do you identify changed records in OLTP?
  - Change Data Capture (CDC)? Timestamp columns? Log-based replication?
  - What's the recovery plan if OLTP database is corrupted?

**SCD Type 2 Implementation:**
- The ETL doc describes SCD Type 2 logic but the actual SQL in star_schema.sql sets `is_current = TRUE` for ALL records. Where's the update logic to expire old records?
- No handling of effective/expiration dates. If a provider changes specialty on Feb 15, how do you attribute January encounters correctly?

**Dependency Management:**
- ETL loads dimensions first, then fact, then bridge. What if a new diagnosis code appears in the fact load before dimension is updated?
- Are there any race conditions if multiple ETL jobs run concurrently?

**Data Quality:**
- No validation that source OLTP data is complete before starting ETL
- No reconciliation queries (e.g., `SUM(fact.revenue) = SUM(OLTP.billing.allowed_amount)`)
- No handling of orphaned records (what if encounter references non-existent patient_id?)

---

### **4. PRODUCTION READINESS**

**Infrastructure (Docker Setup):**
- ✅ Dockerized setup is good for reproducibility
- 🔴 **CRITICAL SECURITY ISSUE:** Hardcoded passwords (`MYSQL_ROOT_PASSWORD: password`) in docker-compose.yml. This WILL fail security review.
- ⚠️ Jupyter notebook has NO authentication (`--NotebookApp.token=''`). Anyone on network can access medical data.
- ⚠️ No resource limits (CPU/memory) in Docker. What if query consumes all RAM?
- ⚠️ MySQL data volume has no backup strategy. Data loss = project failure.

**Scalability:**
- 10K encounters is toy data. How does this perform with:
  - 10M encounters (1000x scale)?
  - 100 concurrent analysts running reports?
  - Real-time dashboard refreshes every 5 minutes?
- Are there partitioning strategies? (e.g., partition fact table by `encounter_date` year/month)
- Missing indexes on bridge tables for reverse lookups (diagnosis → encounters)

**Monitoring & Observability:**
- No logging of ETL execution time, row counts, or failures
- No data quality metrics (e.g., % of encounters with billing data)
- How do you detect if ETL silently produces wrong results?

**Compliance & Security:**
- ⚠️ **HIPAA:** Medical Record Numbers (MRN) and patient names are stored in plaintext. Should these be hashed or encrypted?
- No audit log: who queried patient_id=123's data and when?
- No row-level security: can junior analysts see all patients, or should access be restricted by department?

---

### **5. DOCUMENTATION & KNOWLEDGE TRANSFER**

**Strengths:**
- ✅ Excellent design documentation (design_decisions.txt) with clear rationale
- ✅ Performance analysis (reflection.md) quantifies improvements with actual benchmarks
- ✅ Query analysis identifies OLTP bottlenecks before building solution

**Gaps:**
- 🔴 No runbook: "ETL failed at 3am. What do I do?"
- 🔴 No data dictionary: What does `encounter_type='Outpatient'` mean vs `'ER'`?
- ⚠️ README has setup instructions but no troubleshooting section
- ⚠️ No explanation of business logic (e.g., why is readmission window 30 days, not 60?)

---

### **6. CODE MAINTAINABILITY**

**Modularity:**
- Entire ETL is one 453-line SQL file. Should this be split into:
  - `01_create_dimensions.sql`
  - `02_load_dimensions.sql`
  - `03_create_fact.sql`
  - `04_load_fact.sql`
- No parameterization: Date ranges, database names are hardcoded
- No reusable components: If you add a 5th dimension, you copy-paste the pattern

**Testing:**
- 🔴 **MAJOR GAP:** Zero automated tests
  - No unit tests for ETL transformations
  - No integration tests validating end-to-end pipeline
  - No regression tests to prevent breaking changes
- How do you verify the readmission flag logic is correct?
- What's the test plan before promoting to production?

**Version Control:**
- No .gitignore—are MySQL data files committed?
- No schema versioning (Flyway, Liquibase)—how do you roll back a bad migration?

---

## **DELIVERABLES EXPECTED FROM CODE REVIEW**

As the senior engineer, provide:

1. **Risk Assessment Matrix**
   - **Critical (Blockers):** Issues that MUST be fixed before production
   - **High (Pre-Launch):** Issues to fix in next sprint
   - **Medium (Post-Launch):** Backlog items for future iterations
   - **Low (Nice-to-Have):** Suggestions for long-term improvement

2. **Specific Code Fixes**
   - Line-by-line feedback on SQL queries with corrections
   - Recommended index additions with CREATE INDEX statements
   - Refactored ETL script with error handling

3. **Architecture Recommendations**
   - Alternative design for incremental ETL (CDC vs timestamp-based)
   - Partitioning strategy for fact table
   - Caching layer for frequently-run queries (e.g., Materialized Views)

4. **Production Deployment Checklist**
   - Environment variables for secrets management
   - Backup/restore procedures
   - Performance benchmarks at 1M/10M/100M row scale
   - Monitoring dashboards (query latency, ETL duration, data freshness)

5. **Knowledge Transfer Plan**
   - Onboarding doc for new team members
   - Runbook for common ETL failures
   - Business glossary for domain terms

---

## **EVALUATION CRITERIA**

Use this rubric to score the project (1-10 scale):

| **Category** | **Weight** | **Score** | **Justification** |
|:-------------|:-----------|:----------|:------------------|
| **Data Modeling** | 25% | __/10 | Is the star schema correctly designed per Kimball methodology? |
| **SQL Quality** | 20% | __/10 | Are queries optimized, readable, and production-grade? |
| **ETL Robustness** | 20% | __/10 | Can ETL handle failures, scale, and maintain data quality? |
| **Production Readiness** | 15% | __/10 | Security, monitoring, backups, scalability? |
| **Documentation** | 10% | __/10 | Can a new engineer understand and maintain this? |
| **Code Quality** | 10% | __/10 | Modular, testable, version-controlled? |

**Overall Recommendation:**
- ✅ **APPROVE**: Ship to production with confidence
- ⚠️ **APPROVE WITH CONDITIONS**: Fix critical issues first, then deploy
- 🔴 **REJECT**: Major rework needed before production consideration

---

## **FINAL INSTRUCTIONS**

Be **constructive but critical**. This engineer showed strong foundational skills (good dimensional modeling, clear documentation, performance analysis). However, **production systems require a higher bar**. Your job is to:
- Identify blindspots (security, scalability, failure modes)
- Challenge assumptions (why SCD Type 2 here but not there?)
- Demand evidence (where are the test cases? Where's the disaster recovery plan?)
- Provide mentorship (don't just say "add indexes"—explain WHY and WHERE)

**Remember:** A data warehouse with wrong results is worse than no data warehouse. Prioritize **correctness, reliability, and maintainability** over premature optimization.

---

This prompt should guide a rigorous, real-world code review focused on production readiness, not just academic correctness.