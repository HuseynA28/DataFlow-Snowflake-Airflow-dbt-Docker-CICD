# DATA PIPELINE WITH DBT AND APACHE AIRFLOW IN SNOWFLAKE (CI/CD - GitHub Action)

![dbt Animation](readme_file/Animation_dbt.gif)


The purpose of this project is to automate the ETL process and ensure the data team can validate that the data is in the correct format for the Analytics team to use for machine learning or other analyses. Incorrectly formatted data in the data warehouse can lead to flawed analytical decisions. For this use case, I will utilize the Netflix Movies and TV Shows dataset (https://www.kaggle.com/datasets/shivamb/netflix-shows) to address the following three questions:

- How many new titles does Netflix add each month, and is growth slowing or accelerating?
- Which genres are gaining or losing share of the catalogue over time?
- How does the maturity rating (G, PG‑13, TV‑MA, …) mix evolve by year?

The data is loaded into Snowflake via GitHub Actions. While data is often saved in S3 or modern lakehouse Iceberg tables for analytics, to keep the focus on integrating Snowflake, Airflow, and dbt, a simpler method is used. The notebook `notebooks/snowpark_bootstrap.ipynb` creates a user, database, schema, and tables for dbt.

## GitHub Actions

To run this project, clone the repository and navigate to `/settings/secrets/actions` to add the following secrets:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_WAREHOUSE`

These secrets are retrieved from the repository secrets (see `readme_file/repository_secret.png`). Upon pushing, GitHub Actions will automatically run and create the following:

- `DBT_TARGET_DB`
- `DBT_SCHEMA`
- `NETFLIX_DATA` table, where Netflix data will be stored

For the dbt target, new tables and views will be saved in:

- `DBT_TARGET_DB`
- `DBT_TARGET_SCHEMA`

Additionally, create a virtual environment and install the dependencies listed in the `requirements.txt` file.

## DBT

The dbt project is located within the Airflow project at `dbt-dag/dags/dbt/data_pipeline`. Alternatively, you can create a dbt project outside the Airflow folder, rename it (e.g., `data_pipeline`), and then integrate it with the Airflow project. The dbt project file is currently located at `dags/dbt/data_pipeline` within the Airflow dags folder.

### Project Structure
The `models` folder is divided into three subfolders:

- **staging**: Reads data from the `NETFLIX_DATA` table and splits it into three tables, saved as views. The `NETFLIX_STAGING.yml` file references the data source and includes basic tests. You can edit this in `dbt-dag/dags/dbt/data_pipeline/dbt_project.yml`.
- **clean_data**: Cleans the dataset and applies changes. The `clean_data_source_and_tests.yml` file defines generic tests (e.g., ensuring no missing values or enforcing uniqueness), which are crucial for validating data format from the source.
- **marts**: Contains the `core` folder with a star schema consisting of five dimension tables:
  - `dim_country.sql`
  - `dim_date.sql`
  - `dim_genre.sql`
  - `dim_rating.sql`
  - `dim_type.sql`
  And a fact table:
  - `fct_netflix_title.sql`

In the `business` folder, the following SQL files address the business questions:
- `library_growth_monthly.sql`: Answers “How many new titles does Netflix add each month, and is growth slowing or accelerating?”
- `trends.sql`: Answers “Which genres are gaining or losing share of the catalogue over time?”
- `mix_over_time.sql`: Answers “How does the maturity rating (G, PG‑13, TV‑MA, …) mix evolve by year?”

Note: All dbt-created tables and views are transient and deleted at the end of each session. To retain them, modify `dbt_project.yml` or adjust each model. For example, the `business` models are kept as tables. You can also build a dashboard if desired.

To run the dbt project, configure your `~/.dbt/profiles.yml` file:

```yaml
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
```

## Airflow

Airflow is configured using Astro for its simplicity. Add `dbt-dag/dags/dbt_dag.py` to the dbt project, setting `dbt_project_dir = HERE / "dbt" / "data_pipeline"`. Airflow integrates smoothly with dbt, treating dbt files as tasks. After running, access Airflow on port 8080 to view results (see `readme_file/airflow.png`).

## Why DBT and Airflow?

DBT is essential for transforming raw data into analytics-ready formats by enabling modular SQL-based transformations, testing, and documentation. It ensures data quality through tests defined in `clean_data_source_and_tests.yml`, such as checking for missing values or uniqueness, which prevents downstream issues for the Analytics team. Airflow orchestrates the ETL pipeline, scheduling and managing the dbt tasks to ensure the pipeline runs reliably and on time. Together, they provide a robust framework for automating and monitoring the data pipeline, ensuring the Analytics team receives accurate data for decision-making.

## Business Logic Details

The business logic in the `marts/business` folder directly addresses the three key questions:

- **library_growth_monthly.sql**: Aggregates the number of new titles added to Netflix each month using the `dim_date` dimension and `fct_netflix_title` fact table. It calculates the growth rate by comparing month-over-month changes, helping identify whether Netflix's content expansion is slowing or accelerating.
- **trends.sql**: Analyzes genre distribution over time by leveraging the `dim_genre` dimension and `fct_netflix_title` fact table. It computes the share of each genre in the catalogue annually, revealing which genres are gaining or losing prominence.
- **mix_over_time.sql**: Examines the evolution of maturity ratings (e.g., G, PG-13, TV-MA) by year, using the `dim_rating` dimension and `fct_netflix_title` fact table. It tracks shifts in the proportion of ratings, providing insights into content strategy changes.

These SQL models enable the Analytics team to derive actionable insights for machine learning models or strategic decisions, ensuring data-driven outcomes.

Running the Pipeline
Push to GitHub → GitHub Actions loads the dataset into Snowflake and runs dbt tests.

Start Airflow with astro dev start or docker compose up.

Open Airflow at localhost:8080 and trigger the dbt_dag DAG.  After the running the DAG you will se the result liek below 

![Airflow UI](readme_file/airflow.png)



## Getting Started

1. Clone the repository.
2. Set up GitHub Secrets as described above.
3. Create a virtual environment and install dependencies from `requirements.txt`.
4. Configure `~/.dbt/profiles.yml` with your Snowflake credentials.
5. Run the GitHub Action to load data into Snowflake.
6. Access Airflow on port 8080 to monitor the pipeline.

For further enhancements, consider building a dashboard or modifying dbt models to persist tables.