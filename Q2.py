# Databricks notebook source
sc

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType
from graphframes import GraphFrame

# COMMAND ----------

# Define schema for the edges file: assume each line contains two node IDs separated by whitespace
edge_schema = StructType([
    StructField("src", StringType(), True),
    StructField("dst", StringType(), True)
])

# COMMAND ----------

# Load the edge file (replace 'path_to_edges.txt' with your file path)
edges_df = spark.read \
    .option("delimiter", " ") \
    .schema(edge_schema) \
    .csv("dbfs:/FileStore/tables/0.edges")

# COMMAND ----------

edges_df.show(5)

# COMMAND ----------

# If your dataset is undirected, you can duplicate edges to create a fully directed representation:
rev_edges_df = edges_df.select(col("dst").alias("src"), col("src").alias("dst"))
directed_edges_df = edges_df.union(rev_edges_df)

# COMMAND ----------

rev_edges_df.display()

# COMMAND ----------

directed_edges_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Graphs

# COMMAND ----------

# Create a vertices DataFrame by getting unique node ids from directed_edges_df
vertices_df = directed_edges_df.select("src").union(directed_edges_df.select(col("dst").alias("src"))).distinct().withColumnRenamed("src", "id")

# COMMAND ----------

vertices_df.show(5)

# COMMAND ----------

vertices_df.count()

# COMMAND ----------

# Create the GraphFrame object
g = GraphFrame(vertices_df, directed_edges_df)

# Optionally, you can cache the graph since you'll run multiple algorithms on it.
g.cache()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Running Queries

# COMMAND ----------

# MAGIC %md
# MAGIC 1.Top 5 Nodes with Highest Outdegree

# COMMAND ----------

# Calculate outdegree for each node
out_degrees = g.outDegrees
out_degrees.display()

# COMMAND ----------

# Sort the nodes by outdegree (count of outgoing edges) in descending order and take top 5
top5_out = out_degrees.orderBy(col("outDegree").desc())
top5_out.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC 2.Top 5 Nodes with Highest Indegree

# COMMAND ----------

# Calculate indegree for each node
in_degrees = g.inDegrees
in_degrees.display()

# COMMAND ----------

# Sort the nodes by indegree (count of incoming edges) in descending order and take top 5
top5_in = in_degrees.orderBy(col("inDegree").desc())
top5_in.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC (c) Calculate PageRank and Find Top 5 Nodes

# COMMAND ----------

# Run PageRank with suitable parameters:
# Here, we set a reset probability of 0.15 and run for 1 iterations.
results = g.pageRank(resetProbability=0.15, maxIter=1)

# COMMAND ----------

results.vertices.select("id", "pagerank").show()

# COMMAND ----------

# Extract PageRank scores and sort descending to get the top 5 nodes
top5_pageRank = results.vertices.orderBy(col("pagerank").desc()).limit(5)
top5_pageRank.show()

# COMMAND ----------

# MAGIC %md
# MAGIC (d) Connected Components: Top 5 Largest Components

# COMMAND ----------

spark.sparkContext.setCheckpointDir("dbfs:/tmp/saprk-checkpoints")



# COMMAND ----------

# Run the connected components algorithm
cc = g.connectedComponents()

# COMMAND ----------

# Group by component id and count the nodes in each component
component_sizes = cc.groupBy("component").count()

# COMMAND ----------

# Sort the components by count in descending order and take top 5
top5_components = component_sizes.orderBy(col("count").desc()).limit(5)
top5_components.show()

# COMMAND ----------

# MAGIC %md
# MAGIC (e) Triangle Counts: Top 5 Vertices by Triangle Count

# COMMAND ----------

# Run the triangle count algorithm
triangle_counts = g.triangleCount()

# COMMAND ----------

# Sort vertices by triangle count in descending order and take top 5
top5_triangles = triangle_counts.orderBy(col("count").desc()).limit(5)
top5_triangles.show()