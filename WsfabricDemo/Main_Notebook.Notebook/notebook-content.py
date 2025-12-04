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

# CELL ********************

result = mssparkutils.notebook.run(
    "Utilities_Notebook",
    10,                      # timeout en segundos
    {"a": 10, "b": 20}       # parámetros opcionales
)

print(result)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
