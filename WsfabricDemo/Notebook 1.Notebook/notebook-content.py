# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c01939a0-bfb0-41ca-9644-670d14dfe6ce",
# META       "default_lakehouse_name": "lakehousedemocov",
# META       "default_lakehouse_workspace_id": "55ce2ee2-6ba8-444a-af5d-9ef08b8195db",
# META       "known_lakehouses": [
# META         {
# META           "id": "c01939a0-bfb0-41ca-9644-670d14dfe6ce"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Mi primer notebook
# # A continuación mi código para ejecutar en un cluster de apache spark


# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/datademo/*.csv")
# df now is a Spark DataFrame containing CSV data from "Files/datademo/sales.csv".
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM salesdemo

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
