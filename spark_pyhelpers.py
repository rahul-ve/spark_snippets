import pyspark.sql.functions  as F
from pyspark.sql.types import *

##

# this works same as pandas dataframe[col].map(dict)
from itertools import chain
from pyspark.sql.column import Column

def recode(col_name, map_dict, default=None):
    if not isinstance(col_name, Column):
        col_name = F.col(col_name)
    mapping_expr = create_map([lit(x) for x in chain(*map_dict.items())]) 
    if default is None:
        return  mapping_expr.getItem(col_name)
    else:
        return when(~isnull(mapping_expr.getItem(col_name)), mapping_expr.getItem(col_name)).otherwise(default)

##

# something similar to Pandas.concat() where the columns do not have to be same across the two DFs

def concatDF(leftDF, rightDF):
  leftDF_schema = set((x.name, x.dataType) for x in leftDF.schema)
  rightDF_schema = set((x.name, x.dataType) for x in rightDF.schema)
  
  # get the columns in the RIGHT and add them to LEFT
  for i,j in rightDF_schema.difference(leftDF_schema):
      leftDF = leftDF.withColumn(i,F.lit(None).cast(j))

  # get the columns in the LEFT and add them to RIGHT
  for i,j in leftDF_schema.difference(rightDF_schema):
      rightDF = rightDF.withColumn(i,F.lit(None).cast(j))
  
  concatDF = leftDF.unionByName(rightDF)
  
  return concatDF
