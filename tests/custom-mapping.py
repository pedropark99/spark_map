
def alphabetic_order(index, cols: list, schema):
    cols.sort()
    return cols[index]

# dataframe = spark.table('sales.sales_per_country')
# print(dataframe.columns)



# print(alphabetic_order(1, dataframe.columns, dataframe.schema))
a = ['year', 'month', 'country', 'idstore', 'totalsales']
a.sort()
print(a)
