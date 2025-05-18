DATA PIPELINE WITH DBT AND APACHE AIRFLOW IN SNOWFLAKE (CI/CD - GitHub Action)
The purpose of this project is to automate the ETL process and ensure the data team can validate that the data is in the correct format for the Analytics team to use for machine learning or other analyses. Incorrectly formatted data in the data warehouse can lead to flawed analytical decisions. For this use case, I will utilize the Netflix Movies and TV Shows dataset ([https://www.kaggle.com/datasets/shivamb/netflix-shows](https://www.kaggle.com/datasets/shivamb/netflix-shows)) to address the following three questions:

How many new titles does Netflix add each month, and is growth slowing or accelerating?
Which genres are gaining or losing share of the catalogue over time?
How does the maturity rating (G, PG‑13, TV‑MA, …) mix evolve by year?

The data is loaded into Snowflake via GitHub Actions. While data is often saved in S3 or modern lakehouse Iceberg tables for analytics, to keep the focus on integrating Snowflake, Airflow, and dbt, a simpler method is used. The notebook notebooks/snowpark\_bootstrap.ipynb creates a user, database, schema, and tables for dbt.
GitHub Actions
To run this project, clone the repository and navigate to /settings/secrets/actions to add the following secrets:

SNOWFLAKE\_ACCOUNT
SNOWFLAKE\_PASSWORD
SNOWFLAKE\_ROLE
SNOWFLAKE\_USER
SNOWFLAKE\_WAREHOUSE

These secrets are retrieved from the repository secrets (see readme\_file/repository\_secret.png). Upon pushing, GitHub Actions will automatically run and create the following:

DBT\_TARGET\_DB
DBT\_SCHEMA
NETFLIX\_DATA table, where Netflix data will be stored

For the dbt target, new tables and views will be saved in:

DBT\_TARGET\_DB
DBT\_TARGET\_SCHEMA

Additionally, create a virtual environment and install the dependencies listed in the requirements.txt file.
DBT
The dbt project is located within the Airflow project at dbt-dag/dags/dbt/data\_pipeline. Alternatively, you can create a dbt project outside the Airflow folder, rename it (e.g., data\_pipeline), and then integrate it with the Airflow project. The dbt project file is currently located at dags/dbt/data\_pipeline within the Airflow dags folder.
Project Structure
The models folder is divided into three subfolders:

staging: Reads data from the NETFLIX\_DATA table and splits it into three tables, saved as views. The NETFLIX\_STAGING.yml file references the data source and includes basic tests. You can edit this in dbt-dag/dags/dbt/data\_pipeline/dbt\_project.yml.
clean\_data: Cleans the dataset and applies changes. The clean\_data\_source\_and\_tests.yml file defines generic tests (e.g., ensuring no missing values or enforcing uniqueness), which are crucial for validating data format from the source.
marts: Contains the core folder with a star schema consisting of five dimension tables:
dim\_country.sql
dim\_date.sql
dim\_genre.sql
dim\_rating.sql
dim\_type.sqlAnd a fact table:
fct\_netflix\_title.sql

In the business folder, the following SQL files address the business questions:

library\_growth\_monthly.sql: Answers “How many new titles does Netflix add each month, and is growth slowing or accelerating?”
trends.sql: Answers “Which genres are gaining or losing share of the catalogue over time?”
mix\_over\_time.sql: Answers “How does the maturity rating (G, PG‑13, TV‑MA, …) mix evolve by year?”

Note: All dbt-created tables and views are transient and deleted at the end of each session. To retain them, modify dbt\_project.yml or adjust each model. For example, the business models are kept as tables. You can also build a dashboard if desired.
To run the dbt project, configure your \~/.dbt/profiles.yml file:
data\_flow:
outputs:
dev:
account: XXX-XXX
database: DBT\_TARGET\_DB
password: StrongPassword12345
role: dbt\_role
schema: DBT\_TARGET\_SCHEMA
threads: 1
type: snowflake
user: dbt\_user
warehouse: dbt\_wh
target: dev

Airflow
Airflow is configured using Astro for its simplicity. Add dbt-dag/dags/dbt\_dag.py to the dbt project, setting dbt\_project\_dir = HERE / "dbt" / "data\_pipeline". Airflow integrates smoothly with dbt, treating dbt files as tasks. After running, access Airflow on port 8080 to view results (see readme\_file/airflow\.png).
Why DBT and Airflow?
DBT is essential for transforming raw data into analytics-ready formats by enabling modular SQL-based transformations, testing, and documentation. It ensures data quality through tests defined in clean\_data\_source\_and\_tests.yml, such as checking for missing values or uniqueness, which prevents downstream issues for the Analytics team. Airflow orchestrates the ETL pipeline, scheduling and managing the dbt tasks to ensure the pipeline runs reliably and on time. Together, they provide a robust framework for automating and monitoring the data pipeline, ensuring the Analytics team receives accurate data for decision-making.
Business Logic Details
The business logic in the marts/business folder directly addresses the three key questions:

library\_growth\_monthly.sql: Aggregates the number of new titles added to Netflix each month using the dim\_date dimension and fct\_netflix\_title fact table. It calculates the growth rate by comparing month-over-month changes, helping identify whether Netflix's content expansion is slowing or accelerating.
trends.sql: Analyzes genre distribution over time by leveraging the dim\_genre dimension and fct\_netflix\_title fact table. It computes the share of each genre in the catalogue annually, revealing which genres are gaining or losing prominence.
mix\_over\_time.sql: Examines the evolution of maturity ratings (e.g., G, PG-13, TV-MA) by year, using the dim\_rating dimension and fct\_netflix\_title fact table. It tracks shifts in the proportion of ratings, providing insights into content strategy changes.

These SQL models enable the Analytics team to derive actionable insights for machine learning models or strategic decisions, ensuring data-driven outcomes.
Project Folder Structure
The project directory structure, as shown in the provided images, is organized as follows:

DATAFLOW: Root directory of the project.

NETFLIX\_STAGING.yml: Defines data sources and tests.
stg\_netflix\_titles\_core.sql: Extracts core title data.
stg\_netflix\_titles\_genres\_and\_dates.sql: Extracts genre and date data.
stg\_netflix\_titles\_people.sql: Extracts people-related data (e.g., cast, directors).

clean\_data: Data cleaning and validation layer.
clean\_data\_source\_and\_tests.yml: Defines tests for data quality.
clean\_netflix\_titles\_genres\_and\_dates.sql: Cleans genre and date data.
clean\_netflix\_titles\_people.sql: Cleans people-related data.

marts: Analytics-ready data layer.
business: Business logic for answering key questions.
library\_growth\_monthly.sql: Tracks monthly title additions.
mix\_over\_time.sql: Analyzes maturity rating trends.
trends.sql: Evaluates genre share over time.

core: Star schema for analytics.
dim\_country.sql: Dimension table for countries.
dim\_date.sql: Dimension table for dates.
dim\_genre.sql: Dimension table for genres.
dim\_rating.sql: Dimension table for ratings.
dim\_type.sql: Dimension table for content types (e.g., Movie, TV Show).
fct\_netflix\_title.sql: Fact table linking dimensions.

notebooks: Jupyter notebooks for setup and exploration.
snowpark\_bootstrap.ipynb: Sets up Snowflake user, database, schema, and tables.

Getting Started

Clone the repository.
Set up GitHub Secrets as described above.
Create a virtual environment and install dependencies from requirements.txt.
Configure \~/.dbt/profiles.yml with your Snowflake credentials.
Run the GitHub Action to load data into Snowflake.
Access Airflow on port 8080 to monitor the pipeline.

For further enhancements, consider building a dashboard or modifying dbt models to persist tables.

can you impove the docaution a little withour changing a lot my text  just imove it write it better explain better the ned for dbt and airflow  snowflake intergation which problex thesi use case solve and at the end save it in markdown.md  format the i can use it as a readme.md   it should be in markdown format


Push to GitHub → GitHub Actions loads the dataset and runs dbt tests.

astro dev start (or docker compose up) → open Airflow at localhost:8080 and trigger the dbt_dag.

CI/CD flow
notebooks/snowpark_bootstrap.ipynb provisions Snowflake user + DB + schema.

GitHub Action (.github/workflows/ci.yml)

spins up Snowflake objects

loads the Netflix Movies & TV Shows CSV

executes dbt seed && dbt run && dbt test

Artifacts & test results surface in the pull‑request for instant feedback.

Project layout 🗂️
bash
Copy
Edit
DATAFLOW/
├─ .github/workflows/ci.yml
├─ notebooks/
│  └─ snowpark_bootstrap.ipynb
├─ dags/
│  ├─ dbt_dag.py               # Airflow DAG
│  └─ dbt/
│     └─ data_pipeline/        # dbt project root
│        ├─ models/
│        │  ├─ staging/
│        │  ├─ clean_data/
│        │  └─ marts/
│        │     ├─ core/
│        │     └─ business/
│        └─ dbt_project.yml
└─ requirements.txt
dbt model layers
Layer	Purpose	Key files
staging	Split the raw NETFLIX_DATA table into tidy views.	stg_netflix_titles_*
clean_data	Apply cleaning rules + generic tests (no nulls, uniqueness).	clean_data_source_and_tests.yml
marts / core	Star‑schema dimensions and fact: dim_*, fct_netflix_title.	
marts / business	SQL models that directly answer the three questions.	library_growth_monthly.sql, trends.sql, mix_over_time.sql

All staging & core objects are transient; business models are materialised as tables. Change dbt_project.yml if you need them permanent.

Why dbt + Airflow + Snowflake?
Challenge	How the stack helps
Data quality must be guaranteed before ML uses it.	dbt’s tests (not_null, unique, custom) fail fast in CI and in Airflow runs.
Explainability & lineage for every column.	dbt auto‑generates docs; Airflow UI shows task‑level lineage.
Automated, repeatable deployments across environments.	GitHub Actions builds and tests the same way every time.
Scalability from dev laptop to production.	Snowflake handles volume; Airflow’s scheduler scales horizontally.

Extending the project
Dashboards: Point your BI tool (e.g., Metabase, Tableau) at the marts.business.* tables.

Persist staging layers: change "materialized" in models/staging/*.sql or override in dbt_project.yml.

New tests or sources: add them to *.yml and they’ll run automatically in CI & Airflow.

License & Credits
Dataset © Netflix Shows — Kaggle.
Project structure inspired by the dbt + Airflow best‑practices community.