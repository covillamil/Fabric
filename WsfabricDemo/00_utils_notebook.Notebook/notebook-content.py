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

# 00_utils_notebook

from pyspark.sql import functions as F

def clean_customer_df(df):
    """
    Demostración de función reutilizable:
    - pasa el email a minúsculas
    - crea una columna full_name con nombre + apellido
    """
    return (
        df
        .withColumn("email", F.lower(F.col("email")))
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    )


def calculate_revenue(df, qty_col="quantity", price_col="unit_price"):
    """
    Calcula revenue = quantity * unit_price
    y devuelve un DataFrame con la columna 'revenue'.
    """
    return df.withColumn("revenue", F.col(qty_col) * F.col(price_col))


def say_hello(name: str) -> str:
    """
    Función simple de ejemplo para probar desde otros notebooks.
    """
    return f"Hola {name}, saludo desde 00_utils_notebook 👋"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
