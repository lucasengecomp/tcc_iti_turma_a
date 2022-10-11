import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "libs"))

from pyspark.sql import functions as F

import operations

print("Reading File")
#df = operations.readFile("../sparkTests/csv/olist_order_payments_dataset.csv")
#df.show()

print("Renaming Columns")
#df = operations.renameColumns(df, "payment_value", "amount")
#df.show()

print("Testing Column Operations")
#df = operations.columnsOperations(df, "amount", "amount", "+", "amountSum")
#df = operations.columnsOperations(df, "amount", "amount", "-", "amountSub")
#df = operations.columnsOperations(df, "amount", "amount", "/", "amountDiv")
#df = operations.columnsOperations(df, "amount", "amount", "*", "amountMult")
#df.show()

print("Selecting Specific Columns")
#df = operations.selectColumns(df, ["order_id", "payment_sequential", "payment_type", "payment_installments", "amount"])
#df.show()

print("Filtering Columns")
#df = operations.filterColumn(df, "payment_type", "credit_card", "==")
#df.show()

print("Groupby And Aggregate")
#df = operations.groupbyColumn(df, "order_id", F.sum, "amount")
#df.show()

print("Testing Join")
#df2 = operations.readFile("../sparkTests/csv/olist_orders_dataset.csv")
#df = operations.joinTables(df, df2, "order_id", "order_id", "left")
#df.show()

print("Anonymizing Columns")
#df = operations.anonColumns(df, ["order_id"])
#df.show()

print("Saving Results")
#operations.saveTable(df, "csv", filename="unitTest")
