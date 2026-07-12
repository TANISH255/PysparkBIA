# Databricks notebook source
# MAGIC %md
# MAGIC DATA READING

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/Volumes/workspace/default/pysparkta/BigMart Sales (1).csv")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING, 
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING, 
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE 

                ''' 

# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", True) \
    .schema(my_ddl_schema) \
    .load("/Volumes/workspace/default/pysparkta/BigMart Sales (1).csv")

# COMMAND ----------


df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC StructType() Schema

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import * 

my_strct_schema = StructType([ StructField('Item_Identifier',StringType(),True), StructField('Item_Weight',StringType(),True), StructField('Item_Fat_Content',StringType(),True), StructField('Item_Visibility',StringType(),True), StructField('Item_MRP',StringType(),True), StructField('Outlet_Identifier',StringType(),True), StructField('Outlet_Establishment_Year',StringType(),True), StructField('Outlet_Size',StringType(),True), StructField('Outlet_Location_Type',StringType(),True), StructField('Outlet_Type',StringType(),True), StructField('Item_Outlet_Sales',StringType(),True)

])

df = spark.read.format("csv") \
    .option("header", True) \
    .schema(my_strct_schema) \
    .load("/Volumes/workspace/default/pysparkta/BigMart Sales (1).csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC TRANSFORMATION

# COMMAND ----------

# MAGIC %md 
# MAGIC Select

# COMMAND ----------


df.display()
df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Alias
# MAGIC

# COMMAND ----------

df1 = df.withColumnRenamed("Item_Identifier","ItemID")
#made changes in df1 data frame but not in df
df = df.withColumnRenamed("Item_MRP","Item_Type")
#As we have changed the name and it is string we have to change the schema as well
my_ddl_schema1 = '''
Item_Identifier STRING,
Item_Weight DOUBLE,
Item_Fat_Content STRING,
Item_Visibility DOUBLE,
Item_Type STRING,
Item_MRP DOUBLE,
Outlet_Identifier STRING,
Outlet_Establishment_Year INT,
Outlet_Size STRING,
Outlet_Location_Type STRING,
Outlet_Type STRING,
Item_Outlet_Sales DOUBLE
'''
df = spark.read.format("csv") \
    .option("header", True) \
    .schema(my_ddl_schema1) \
    .load("/Volumes/workspace/default/pysparkta/BigMart Sales (1).csv")

# COMMAND ----------

df.display()

# COMMAND ----------

#FILTER
df.filter(col('Item_Fat_Content')=='Regular').display()
     
     

# COMMAND ----------


df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

#withColumnRenamed

df.withColumnRenamed('Item_Weight','Item_Wt').display()

# COMMAND ----------

#But the change of column name isn't in the original one :
df.display()

# COMMAND ----------

#withColumn


df = df.withColumn('flag',lit("new")) 
#Lit is a function which will populate all the rows with the same value (Constant value)

df.display()
     

df.withColumn('multiply',col('Item_Weight')*col('Item_MRP')).display()
#Will add a new column multiply to the last in next result below we can see that.

# COMMAND ----------

#To make changes in the original dataframe , regexp_replace is used to change column value name to other name 
df = df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat","Lf"))

df.display()

# COMMAND ----------

#Type casting the method we used in upper part of the code to change schema first and then apply the schema is actually not a good practice we #can do this to type cast instead.

df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())) 

df.printSchema()

# COMMAND ----------

#Sorting
#1
df.sort(col('Item_Weight').desc()).display()
     
#2
df.sort(col('Item_Visibility').asc()).display()
     

# COMMAND ----------


#Casting we did in the uppar part to show a method we will take it back to normal 
df = df.withColumn('Item_Weight', col('Item_Weight').cast(DoubleType()))

df.sort(['Item_weight','Item_Visibility'], ascending = [0,1]).display()


# COMMAND ----------

#Here it is for multiple columns sorting such that False,False for both it means descending order
df.sort(['Item_Weight','Item_Visibility'],ascending = [0,0]).display() 

# COMMAND ----------

# Limit

df.limit(10).display() 

# COMMAND ----------


#DROP
df.drop('Item_Visibility').display()
     
df.drop('Item_Visibility','Item_Type').display()
     

# COMMAND ----------

#Drop_Duplicates

df.dropDuplicates().display()
#It will drop the columns where it is null in all columns 

# COMMAND ----------

#Only null in specific columns are dropped  
df.drop_duplicates(subset=['Item_Type']).display()
#Drop only if all rows are null 
df.distinct().display()

# COMMAND ----------

#Data frames 
data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

#Union
df1.union(df2).display()
     

data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()
     

df1.union(df2).display()

# COMMAND ----------

#As we can see oddly in upper second result for by name results 
df1.unionByName(df2).display()

# COMMAND ----------

#String Functions
#Makes FrUitS to Fruits
df.select(initcap('Item_Type').alias('initcap_Item_Type')).display()
#Upper and lower 
df.select(upper('Item_Type').alias('upper_Item_Type')).display()
df.select(lower('Item_Type').alias('lower_Item_Type')).display()


# COMMAND ----------

#Date Functions

df = df.withColumn('curr_date',current_date())
df = df.withColumn('week_after',date_add('curr_date',7)) 
# same for below one : df.withColumn('week_before',date_sub('curr_date',7)).display()
df = df.withColumn('week_before',date_add('curr_date',-7)) 


df.display() 


# COMMAND ----------

#DateDIFF

df = df.withColumn('datediff',datediff('week_after','curr_date'))

#Date_Format()

df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))

df.display()

# COMMAND ----------

#Handling Nulls
#Dropping NUlls
#All nulls are gone
df.dropna('all').display()

#Any null is gone   
df.dropna('any').display()
     
#Only nulls in columns are gone
df.dropna(subset=['Outlet_Size']).display()
df.display()
     


# COMMAND ----------

#Not to put data into risk we use fill nulls 
#Filling Nulls
#Fill all nulls with this :
df.fillna('NotAvailable').display()
#We are able to see nulls in item weight as it is double 

#fill only nulls in specific columns
df.fillna('NotAvailable',subset=['Outlet_Size']).display()
     


# COMMAND ----------

#SPLIT and Indexing
#SPLIT

#Split by dilimeter space
df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()
     
#Indexing
#Normal py indexing [0.1]
df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

#Split
df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))     

#Explode here as it is supermarket and type1 for instance one row is supermarket one is type1
df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()
     

df_exp.display()
     
#Array contains true if contains
df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

#Group By  
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

#Collect List
#Creates list of items after grouping by.
data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)     

df_book.groupBy('user').agg(collect_list('book')).display()
     

# COMMAND ----------

#pivot 
df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------



# COMMAND ----------

#When-Otherwise
from pyspark.sql.functions import col, when
df = df.withColumn('Veg_flag',when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Veg')).display()

# COMMAND ----------

#joins
dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)
     

# COMMAND ----------

df1.join(df2,df1['dept_id'] == df2['dept_id'],'inner').display()

df1.join(df2,df1['dept_id'] == df2['dept_id'],'left').display()

df1.join(df2,df1['dept_id'] == df2['dept_id'],'right').display()

df1.join(df2,df1['dept_id'] == df2['dept_id'],'anti').display()
#Anti is row in df1 which is not in df2

# COMMAND ----------

df.display()

# COMMAND ----------

#Window functions
from pyspark.sql.window import Window 
df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('dense_rank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()


# COMMAND ----------

df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("/Volumes/workspace/default/pysparkta/BigMart Sales (1).csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

df.withColumn('dum',sum('Item_MRP').over(Window.orderBy('Item_Identifier').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

#Cumulative Sum 

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()
     
     

# COMMAND ----------

#User defined functions (UDF's)

def myfun(x):
    return x*x

my_udf = udf(myfun) 

df.withColumn('mynewcol',my_udf(col('Item_MRP'))).display()


# COMMAND ----------

#Data Writing

#CSV
df.write.format('csv')\
    .save('/Volumes/workspace/default/pysparkta/mycsv',header=True)

# COMMAND ----------

#Append 
df.write.format('csv')\
    .mode('append')\
    .save('/Volumes/workspace/default/pysparkta/mycsv',header=True)


# COMMAND ----------

#Ovwewrite
df.write.format('csv')\
    .mode('overwrite')\
    .save('/Volumes/workspace/default/pysparkta/mycsv',header=True)


# COMMAND ----------

#Error
df.write.format('csv')\
    .mode('error')\
    .save('/Volumes/workspace/default/pysparkta/mycsvdefault',header=True)

# COMMAND ----------

#Ignore
df.write.format('csv')\
    .mode('ignore')\
    .save('/Volumes/workspace/default/pysparkta/mycsv',header=True)

# COMMAND ----------

#Parquet
df.write.format('parquet')\
    .save('/Volumes/workspace/default/pysparkta/mycsv_parquet',header=True)


# COMMAND ----------

df.write.format('delta')\
  .mode('overwrite')\
  .saveAsTable('mytable') 

df.display()

# COMMAND ----------

#Spark SQL
#create temp table 

df.createTempView('my_view')



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content = 'Regular'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Regular'")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

