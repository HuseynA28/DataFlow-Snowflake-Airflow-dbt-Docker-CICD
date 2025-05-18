DATA PIPELINE WITH DBT AND APACHE AIRFLOW ON SNOWFLAKE (CI/CD – GitHub Actions)
The purpose of this project is to demonstrate an automated ETL pipeline with data -quality tests, ensuring that the Analytics team receives data in the correct format for machine‟learning and other downstream use cases. When data in the warehouse is poorly formatted it can lead to wrong analytical decisions.

For this example we use the Netflix Movies and TV Shows dataset (https://www.kaggle.com/datasets/shivamb/netflix-shows) and answer three questions:

How many new titles does Netflix add each month, and is growth slowing or accelerating?

Which genres are gaining or losing share of the catalogue over time?

How does the maturity rating (G, PG‑13, TV‑MA, …) mix evolve by year?

Data loading
Data is loaded to Snowflake via a GitHub Action. In production you might land raw data in S3 or a modern lakehouse format such as Iceberg, but to keep things simple and focus on the integration between Snowflake, Airflow, and dbt we load directly from the repository. The bootstrap script lives in notebooks/snowpark_bootstrap.ipynb; it also creates the Snowflake user, database, schema, and table required by dbt.

GitHub Actions
To run this project yourself:

Clone the repository.

Go to Settings → Secrets → Actions and add:

nginx
Copy
Edit
SNOWFLAKE_ACCOUNT
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_USER
SNOWFLAKE_WAREHOUSE
The workflow reads these values from repository secrets. When you push to main, the action runs automatically and creates:

DBT_TARGET_DB

DBT_SCHEMA

the source table NETFLIX_DATA (raw dataset)

All dbt models will be written to:

DBT_TARGET_DB

DBT_TARGET_SCHEMA

Local setup
Create a virtual environment and install the dependencies:

bash
Copy
Edit
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
dbt project structure
The dbt project is located at dbt-dag/dags/dbt/data_pipeline. (If you prefer, scaffold the project elsewhere and then move it into the Airflow folder.)

pgsql
Copy
Edit
models/
│
├─ staging/           -- read from NETFLIX_DATA and split into three staging views
│   └─ NETFLIX_STAGING.yml   -- source definitions + basic tests
│
├─ clean_data/        -- cleans the dataset and adds validation tests
│   └─ clean_data_source_and_tests.yml
│
└─ marts/
    ├─ core/          -- star schema
    │   ├─ dim_country.sql
    │   ├─ dim_date.sql
    │   ├─ dim_genre.sql
    │   ├─ dim_rating.sql
    │   ├─ dim_type.sql
    │   └─ fct_netflix_title.sql
    │
    └─ business/
        ├─ library_growth_monthly.sql   -- Q1
        ├─ trends.sql                   -- Q2
        └─ mix_over_time.sql            -- Q3
All dbt models are transient, so tables and views are dropped at the end of each session. If you want to keep them, change dbt_project.yml or override the materialization at model level (the business layer is currently materialized as tables).

Update ~/.dbt/profiles.yml:

yaml
Copy
Edit
data_flow:
  outputs:
    dev:
      account: XXX-XXX
      database: DBT_TARGET_DB
      password: StrongPassword12345
      role: dbt_role
      schema: DBT_TARGET_SCHEMA
      threads: 1
      type: snowflake
      user: dbt_user
      warehouse: dbt_wh
  target: dev
Apache Airflow
Airflow is configured with Astronomer because it makes local development easy. Add dbt-dag/dags/dbt_dag.py to your DAGs folder and set

python
Copy
Edit
dbt_project_dir = HERE / 'dbt' / 'data_pipeline'
Airflow picks up every dbt command as a separate task. After starting Airflow (astro dev start) open http://localhost:8080 to view the DAG. A sample run is shown in readme_file/airflow.png.

(Optional) Build a dashboard on top of the business‟layer tables—e.g. in Looker or Tableau.
