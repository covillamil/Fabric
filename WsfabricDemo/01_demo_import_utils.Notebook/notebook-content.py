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

import sys

# Ruta a la carpeta Files del Lakehouse
sys.path.append("/lakehouse/default/Files")

import utils



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row

data = [
    Row(first_name="Cesar",  last_name="Villamil", email="CESAR@EXAMPLE.COM", quantity=2, unit_price=100.0),
    Row(first_name="Ana",    last_name="Lopez",    email="ANA@EXAMPLE.COM",   quantity=1, unit_price=250.0),
    Row(first_name="Marcos", last_name="Soto",     email="MARCOS@EXAMPLE.COM",quantity=5, unit_price=80.0),
]

df_raw = spark.createDataFrame(data)
display(df_raw)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Usar la función de limpieza
df_clean = utils.clean_customer_df(df_raw)
display(df_clean)

# Usar la función de revenue
df_with_rev = utils.calculate_revenue(df_clean)
display(df_with_rev)

# Usar la función de saludo
print(utils.say_hello("César"))


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
