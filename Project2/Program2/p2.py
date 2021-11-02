#!/usr/bin/env python
# coding: utf-8
# In[1]:
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import numpy
# In[2]:
import findspark
findspark.init(r'/home/ubuntu/hadoop/spark')
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
# In[3]:
df_log = spark.read.csv('hdfs:///user/root/accesslog/access_log', header=False, inferSchema=True, sep=' ')
df_log.show()

# In[4]:
df1 = df_log.select("_c0", F.regexp_replace(F.col("_c3"), "\[", "").alias("Time"))
df1.show()


# In[5]:


df_model = df1.select("_c0", F.to_timestamp(df1.Time, 'dd/MMM/yyyy:HH:mm:ss').alias('date'))
df_model.show()


# In[6]:


df = df_model.withColumn('year',F.to_date(df_model.date)).groupby('year').count()
df = df.withColumn("year", F.unix_timestamp("year", "dd-MM-yyyy")/10000000000)
df = df.dropna()
df.show()


# In[7]:


feature_columns = df.columns[:-1]
assembler = VectorAssembler(inputCols=feature_columns,outputCol="input")
data_2 = assembler.transform(df)
data_2.show()


# In[8]:


train, test = data_2.randomSplit([0.8, 0.2])
lin_reg = LinearRegression(featuresCol="input", labelCol="count", predictionCol='prediction')
model = lin_reg.fit(train)
eval_summary = model.evaluate(test)


# In[9]:


print(model.summary.meanAbsoluteError)
print(eval_summary.rootMeanSquaredError)
print(eval_summary.r2)


# In[10]:


predictions = model.transform(test)
predictions.select(predictions.columns).show()


# In[11]:


path = 'linear_reg_spark_part3_model'
model.write().overwrite().save(path)


# In[ ]:




