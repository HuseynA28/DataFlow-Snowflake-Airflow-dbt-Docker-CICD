

#  DATA PIPELINE WITH DBT AND APACHE AIRFLOW in Snowflake(CICD -github action)
The purpose of this progect is prepare a use that the data goinf the ETL auamatily  and also make the  test that the The data team make sure that the data is on righ forma that Anataics team can use it for machine leaning or out thigs . Something the data in data warehouse is not correctly format and it lead to wrong anataics deciation . FOr this use  case I will us ethe   Netflie Movidela nd TV shows that data set (https://www.kaggle.com/datasets/shivamb/netflix-shows) and try to find these thre question 

How many new titles does Netflix add each month, and is growth slowing or accelerating ?
Which genres are gaining or losing share of the catalogue over time?
How does the maturity rating (G, PG‑13, TV‑MA, …) mix evolve by year?

The data load to snowflake  via github action . mostly the data is saved in S3 or even  in moden lakehouse iceberg tables is also used for anatlics purpose  but in order to keep the data part simple and focuce on intergation Snowflake , Airflow and dbt I use simple method . notebooks\snowpark_bootstrap.ipynb   in this notebook I also create a user and database , schema table for the dbt . 

Github action 
If you would like to run thhis project  you need to clone  and after  that togo /settings/secrets/actions  the add 

SNOWFLAKE_ACCOUNT
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_USER
SNOWFLAKE_WAREHOUSE

The gihub will take the secret from  repository secrets 
readme_file\repository secret.png . If you push  the action will run auatoialy and will crteat  the below database 
DBT_TARGET_DB
DBT_SCHEMA
 NETFLIX_DATA  table and teh Netflis data will save there   

 for teh taget dbt that new table and view be saved in 

  DBT_TARGET_DB
  DBT_TARGET_SCHEMA 


YOU need also create a virual entiment and install the requirements.txt  file 
DBT 
dbt-dag\dags\dbt\data_pipeline
The dbt project in localed in airflow project . In data_pipleine . If  you would like we can fisrt create a dbt project outsie the airflow folder the rename it as data_pipnline or or something different  then can add the airflow project .  Just I wwan to  stell yo  that I dbt project file is located dags\dbt\data_pipeline   in airflow daags folder.

I have 3 different folder in models 

staging
Whre I readt the data from NETFLIX_DATA table and split  them into 3  tables.NETFLIX_STAGING.yml the file that we can use the refer the data source and  write some test .There I used just poin the data source in 3 stg  file . After  spliting the data into 3 tables I saved them as a view . YOu can edit it from dbt-dag\dags\dbt\data_pipeline\dbt_project.yml

clean_data
where I clean the dataset and make some change  and also  clean_data_source_and_tests.yml  in this file define  some generet  test that for examle some column should not have the missign  value or should be always the uniq . This test is very imporatn becuase it could be that the data form soruce could be in wrong format this test help to to make sure thet everthing is correct 

marts  
in mart folde we have the core we I bbuild teh start schame  with 5 dimtaion table 
dim_country
dim_data
dm_genre
dim_rating
dim_type
 and we have a  fact table with the name  fct_netflix_title.sql  

 in buiness 
 we have 

library_growth_monthly.sql  help us fin dthe  adnwere  Business question – “How many new titles does Netflix add each month, and is growth slowing or accelerating?”

trends.sql “Which genres are gaining or losing share of the catalogue over time?”
mix_over_time.sql  “How does the maturity rating (G, PG‑13, TV‑MA, …) mix evolve by year?” 

The import thin is that all of teh dbt create table and view are transient  at the end ogf each session they will be deleted . if you wan to keeop them you can chnage dbt_project.yml  or do in in each model For explain I keep the business as a tabel 
If you would like you can also build a bashabord  
If you would like to run my project do not forget add or edit you code ~/.dbt/profiles.yml

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


Airflow

I used the astro  for airflow confiqution ebcsue astrois easy 
We need jus addd the  dbt-dag\dags\dbt_dag.py to dbt  project  place dbt_project_dir = HERE / "dbt" / "data_pipeline". The Airflow and dbt interagtion is very smully airflow aualy get the dbt file as task . After the running we open the port 8080 to see airflow and after the running  teh result will be like bewlo readme_file\airflow.png