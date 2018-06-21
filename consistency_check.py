# External souce package for python is installed, as example Teradata and hive
!pip install teradata;
!pip install PyHive;
import teradata as td;
import pandas as pd;
from pyhive import hive;
from google.cloud import bigquery;

# setting connection to source/layer 1, example teradata
udaExec_l1 = td.UdaExec(appName="consistency_validation", version="1.0", logConsole=False)
conn_layer1 = udaExec.connect(method="odbc", system="xxxxx", username="xxx", password="xxx")

# setting connection to process layer 2, example hive data lake
conn_layer2 = hive.Connection(host="xxxxx", port=xxxx, username="xxxx")

# setting connection to process layer 3, example GCP BigQuery
conn_layer3 = bigquery.Client()

# it assumes there is a total of XX consistency checks represented on the total 
total_checks = 20

for i in range(1, total_checks):
  table_name = 'consistency_table_' + str(i)
# first level validation is to retrieve the query with the agregated figures of the "landing data layer" which must be consitent with the source data
  results_layer1 = cursor_layer1.execute("SELECT * FROM ${table_name}",continueOnError=True)
  df_layer1 = pd.DataFrame(results_layer1.fetchall())
  df_layer1.columns = results_layer1.keys()
  
# second level validation is to retrieve the query with the agregated figures of the "transformation layer" which must be consitent with the landing layer
  df_layer2 = pd.read_sql("SELECT cool_stuff FROM ${table_name}", conn)
  
# third level validation is to retrieve the query with the agregated figures of the "consumption layer" which must be consitent with the transformation layer
# This is examplified deom GCP BigQuery
  query_job = client.query("""SELECT * FROM ${table_name}""")
  df_layer3 = pd.DataFrame(query_job.result())
  df_layer3.columns = results_layer1.keys()
  
# Now it transform the data for the result table, been this generic meant to be utilized as a non-specifi framework the data needs to mutate to suit this  

# Per consistency layer there is one record to be put together to be inserted in the final BigQuery consistency check dataset

##### LAYER 1 mutating ans storing result #####
# First the day of the consistency check
  today = now.strftime("%Y-%m-%d")
  bq_record_l1[0] = today

# now the query ID
  bq_record_l1[1] = table_name
  
# the data layer the record belongs
  bq_record_l1[3] = 'layer1'
  
# Now a loop to all the columns and values
  for j in range (0, len(df_layer1.columns)-1):
    bq_record_l1[4+j] = df_layer1.columns[j]
    bq_record_l1[4+j+1] = df_layer1.iat[0,j]
    
 # finally it insert the rows on result table on BigQuery
  client.insert_rows('consistency_check_results', [bq_record_l1])
  
  ##### LAYER 2 mutating ans storing result #####
# First the day of the consistency check
  today = now.strftime("%Y-%m-%d")
  bq_record_l2[0] = today

# now the query ID
  bq_record_l2[1] = table_name
  
# the data layer the record belongs
  bq_record_l2[3] = 'layer2'
  
# Now a loop to all the columns and values
  for j in range (0, len(df_layer2.columns)-1):
    bq_record_l21[4+j] = df_layer2.columns[j]
    bq_record_l1[4+j+1] = df_layer2.iat[0,j]
    
 # finally it insert the rows on result table on BigQuery
  client.insert_rows('consistency_check_results', [bq_record_l2])
    
  ##### LAYER 3 mutating ans storing result #####
# First the day of the consistency check
  today = now.strftime("%Y-%m-%d")
  bq_record_l3[0] = today

# now the query ID
  bq_record_l3[1] = table_name
  
# the data layer the record belongs
  bq_record_l3[3] = 'layer1'
  
# Now a loop to all the columns and values
  for j in range (0, len(df_layer3.columns)-1):
    bq_record_l3[4+j] = df_layer3.columns[j]
    bq_record_l3[4+j+1] = df_layer3.iat[0,j]
    
 # finally it insert the rows on result table on BigQuery
  client.insert_rows('consistency_check_results', [bq_record_l3])
