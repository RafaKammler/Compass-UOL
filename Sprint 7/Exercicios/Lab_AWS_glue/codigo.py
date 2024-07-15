import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import upper, col
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_TARGET_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

pasta_destino = args['S3_TARGET_PATH']

dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [args["S3_INPUT_PATH"]]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
)

df = dynamic_frame.toDF()

print("Dataframe Schema:")
df.printSchema()

print("Dataframe Columns:")
print(df.columns)

df_upper_df = df.withColumn("nome", upper(df["nome"]))
print(df_upper_df.columns)

print("Linhas do dataframe:")
print(df_upper_df.count())

df_upper_df.printSchema()

if 'ano' not in df_upper_df.columns or 'sexo' not in df_upper_df.columns:
    raise ValueError("As colunas 'ano' e 'sexo' não existem no dataframe")

grouped_df = df_upper_df.groupBy("ano", "sexo").count()
print("Contagem de nomes por ano e sexo:")
grouped_df.show()

sorted_df = df_upper_df.sort("ano")

feminine_df = df_upper_df.filter(df_upper_df.sexo == "F")
feminine_max_df = feminine_df.orderBy(col('total').desc())
print("Nome feminino mais frequente:")
feminine_max_df.show(1)

masculine_df = df_upper_df.filter(df_upper_df.sexo == "M")
masculine_max_df = masculine_df.orderBy(col('total').desc())
print("Nome masculino mais frequente:")
masculine_max_df.show(1)

yearly_count_df = df_upper_df.groupBy("ano").count()
print("Registros por ano:")
yearly_count_df.show()

top_10_df = sorted_df.limit(10)
print("10 primeiras linhas:")
top_10_df.show()

top_10_dynamic_frame = DynamicFrame.fromDF(top_10_df, glueContext, "top_10_dynamic_frame")

df_upper_df.write.mode("overwrite").partitionBy('sexo', 'ano').json(pasta_destino)

job.commit()
