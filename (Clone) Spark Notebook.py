# Databricks notebook source
display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

f = open('/dbfs/databricks-datasets/flights/README.md', 'r')
print(f.read())

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/flights/")


# COMMAND ----------

df = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/flights/departuredelays.csv")
)

# COMMAND ----------

df.display()

# COMMAND ----------

df = spark.read.text('dbfs:/databricks-datasets/flights/airport-codes-na.txt')

# COMMAND ----------

df.display()

# COMMAND ----------

tripdelaysFilePath = "/databricks-datasets/flights/departuredelays.csv"
airportsnaFilePath = "/databricks-datasets/flights/airport-codes-na.txt"

# COMMAND ----------

airports = (spark.read
            .format("csv")
            .options(header='true', inferschema='true', delimiter='\t')
            .load(airportsnaFilePath)
           )



# COMMAND ----------

airports.display()
airports.count()

# COMMAND ----------

airports.createOrReplaceTempView("airports_na")

# COMMAND ----------

df = spark.sql("select * from airports_na")
df.display()

# COMMAND ----------

spark.catalog.dropTempView("airports_na")

# COMMAND ----------

departure_delays = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/flights/departuredelays.csv")
)

# COMMAND ----------

departure_delays.display()

# COMMAND ----------

# register temporary table
departure_delays.createOrReplaceTempView("DepartureDelays")

# COMMAND ----------

# cache() method saves dataframe to storage level `Memory_and_disk` because recomputing the in-memory columnar represntation of the underlying table is expensive
# yields as protected resource and caches the current dataframe
# until gets uncaches after execution goes out the contenxt
# to uncache dataframe use df.unpersist()
departure_delays.cache()

# COMMAND ----------

iata_origin = departure_delays.select("origin").distinct().withColumnRenamed("origin", "IATA_temp")

# COMMAND ----------

iata_destination = departure_delays.select("destination").distinct().withColumnRenamed("destination", "IATA_temp")

# COMMAND ----------

iata_trip = iata_origin.union(iata_destination).distinct()


# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, concat, lit, to_timestamp, when, length, size, lpad
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
airports = airports.alias('o').select("o.IATA", "o.City", "o.State", "o.Country").join(iata_trip.alias('f') , col('o.IATA') == col('f.IATA_temp'))
airports = airports.drop('IATA_temp')
airports.display()

airports.registerTempTable("airports")
airports.cache()
airports.count()

# COMMAND ----------

airports.display()

# COMMAND ----------

departure_delays = departure_delays.withColumnRenamed("date", "trip_id")


# COMMAND ----------

departure_delays.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, concat, lit, to_timestamp, when, length, size, lpad
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

departure_delays = (spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/flights/departuredelays.csv")
)

departure_delays = departure_delays.withColumnRenamed("date", "trip_id")



departure_delays = departure_delays.withColumn("trip_id", departure_delays.trip_id.cast("string"))


#temp = departure_delays.filter(length('trip_id') < 8).select(concat(lit("0"), col("trip_id")).alias("local_date"))

departure_delays = departure_delays.withColumn('local_date', lpad(departure_delays['trip_id'], 8, '0'))
                                               
#departure_delays.withColumn('trip_id', when(length(col('trip_id')) < 8, regexp_replace('trip_id', '0', 'trip_id'))).display()
#departure_delays.join(temp).display()

departure_delays = departure_delays.withColumn('local_date',(to_timestamp(concat(lit("2014"), lit("-"),departure_delays.local_date.substr(1,2),lit("-"), departure_delays.local_date.substr(3,2), lit(" "), departure_delays.local_date.substr(5,2), lit(":"), departure_delays.local_date.substr(7,2), lit(":00"))).alias("local_date")))

departure_delays.display()


#departure_delays.join(temp, departure_delays.trip_id == temp.trip_id, how='inner').drop(display()





#month
#month = temp.select(temp.date.substr(1,2))



#temp.select(concat(month, '-')).display()

# COMMAND ----------

departure_delays.show()

# COMMAND ----------

departure_delays = departure_delays.withColumnRenamed("origin", "src")
departure_delays = departure_delays.withColumnRenamed("destination", "dst")



# COMMAND ----------

departure_delays.where(col('src')=='ABE').display()
departure_delays.display

# COMMAND ----------

airports.display()
airports.drop('iata').display()

# COMMAND ----------

departure_delays_geo = departure_delays.join(airports.alias('f'), departure_delays.src == col("f.IATA")).join(airports.alias('o'), departure_delays.dst == col("o.IATA")).select(col("trip_id"), col("delay"), col("distance"), col("src"), col("dst"), col("local_date"), col("f.City").alias("city_src"), col("f.State").alias("state_src"), col("o.City").alias("city_dst"), col("o.State").alias("state_dst"))
departure_delays_geo.where(col('src') == 'ABE').display()


# COMMAND ----------

# register temporary table
departure_delays_geo.createOrReplaceTempView("DepartureDelays_geo")

# COMMAND ----------

departure_delays_geo.cache()
departure_delays_geo.count()

# COMMAND ----------

# Building the Graph
# Now that we've imported our data, we're going to need to build our graph. To do so we're going to do two things. We are going to build the structure of the vertices (or nodes) and we're going to build the structure of the edges. What's awesome about GraphFrames is that this process is incredibly simple.

# Rename IATA airport code to **id** in the Vertices Table
# Start and End airports to **src** and **dst** for the Edges Table (flights)


# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python3/bin/pip install graphframes

# COMMAND ----------

from pyspark.sql.functions import *
from graphframes import *

# COMMAND ----------

trip_vertices = airports.withColumnRenamed("IATA", "id").distinct()
trip_edges = departure_delays_geo.select('trip_id', 'delay', 'src', 'dst', 'city_dst', 'state_dst')


# COMMAND ----------

trip_vertices.cache()
trip_edges.cache()

# COMMAND ----------

trip_vertices.display()

# COMMAND ----------

trip_edges.display()

# COMMAND ----------

# MAGIC %sh
# MAGIC ./bin/pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11

# COMMAND ----------

from graphframes import *
trip_graph = GraphFrame (trip_vertices, trip_edges)
print(trip_graph)

# COMMAND ----------

trip_edges_subset = departure_delays_geo.select('trip_id', 'delay','src', 'dst')
trip_graph_subset = GraphFrame (trip_vertices, trip_edges_subset)

# COMMAND ----------

# Determine number of airports and flights
print(f'Airports:{trip_graph.vertices.count()}')
print(f'Trips:{trip_graph.edges.count()}')

# COMMAND ----------

# Determine the longest delay in the dataset
longest_delay = trip_graph.edges.groupBy().max("delay")
longest_delay.display()

# COMMAND ----------

# Determine number of delayed vs on time & early flights

print(f'On-time & Early flights: {trip_graph.edges.filter("delay <= 0").count()}')
print(f'Delayed flights: {trip_graph.edges.filter("delay > 0").count()}')

# COMMAND ----------

# What flights departing SFO are most likely to have a significant delay?

trip_graph.edges.filter("src = 'SFO' and delay > 0").groupBy("src", "dst").avg("delay").sort(desc("avg(delay)")).display()



# COMMAND ----------

# what destinations tend to have delays?

trip_delays = trip_graph.edges.filter("delay > 0")
display(trip_graph.edges.filter("delay > 0"))

# COMMAND ----------

# States with the longest cumulative delays (with individual delays > 100 minutes) from origin:Seattle
display(trip_graph.edges.filter("src = 'SEA' and delay > 100"))

# COMMAND ----------

#Vertex Degrees
#inDegrees: Incoming connections to the airport
#outDegrees: Outgoing connections from the airport
#degrees: Total connections to and from the airport
#Reviewing the various properties of the property graph to understand the incoming and outgoing connections between airports.

# COMMAND ----------

# Degrees
# The number of degrees - the number of incoming and outgoing connections - for various airports within this sample dataset
display(trip_graph.degrees.sort(desc("degree")).limit(20))

# COMMAND ----------

# What delays might we blame on SFO

# Using tripGraphPrime to more easily display 
#   - The associated edge (ab, bc) relationships 
#   - With the different the city / airports (a, b, c) where SFO is the connecting city (b)
#   - Ensuring that flight ab (i.e. the flight to SFO) occured before flight bc (i.e. flight leaving SFO)
#   - Note, TripID was generated based on time in the format of MMDDHHMM converted to int
#       - Therefore bc.tripid < ab.tripid + 10000 means the second flight (bc) occured within approx a day of the first flight (ab)
# Note: In reality, we would need to be more careful to link trips ab and bc.

# COMMAND ----------


motifs = trip_graph_subset.find("(a)-[ab]->(b); (b)-[bc]->(c)").filter("(b.id = 'SFO') and (ab.delay > 500 or bc.delay > 500) and bc.trip_id > ab.trip_id and bc.trip_id < ab.trip_id + 10000")
display(motifs)



# COMMAND ----------

# Determining Airport Ranking using PageRank
# There are a large number of flights and connections through these various airports included in this Departure Delay Dataset.  Using the `pageRank` algorithm, Spark iteratively traverses the graph and determines a rough estimate of how important the airport is.

# Determining Airport ranking of importance using `pageRank`

# COMMAND ----------

ranks = trip_graph.pageRank(resetProbability=0.15, maxIter=5)
display(ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(20))

# COMMAND ----------


# Most popular flights (single city hops)
# Using the `tripGraph`, we can quickly determine what are the most popular single city hop flights

# Determine the most popular flights (single city hops)

# COMMAND ----------

top_trips = trip_graph.edges.groupBy("src", "dst").agg(count("delay").alias("trips"))

# COMMAND ----------

display(top_trips.orderBy(top_trips.trips.desc()).limit(20))

# COMMAND ----------

# Top Transfer Cities
# Many airports are used as transfer points instead of the final Destination.  An easy way to calculate this is by calculating the ratio of inDegree (the number of flights to the airport) / outDegree (the number of flights leaving the airport).  Values close to 1 may indicate many transfers, whereas values < 1 indicate many outgoing flights and > 1 indicate many incoming flights.  Note, this is a simple calculation that does not take into account of timing or scheduling of flights, just the overall aggregate number within the dataset.

# Calculate the inDeg (flights into the airport) and outDeg (flights leaving the airport)

# COMMAND ----------

in_deg = trip_graph.inDegrees
out_deg = trip_graph.outDegrees

# COMMAND ----------

# Calculate degree ratio

degree_ratio = in_deg.join(out_deg, in_deg.id == out_deg.id).drop(out_deg.id).selectExpr('id', 'double(inDegree)/double(outDegree) as degree_ratio').cache()


# COMMAND ----------

# Join back to the `airports` DataFrame (instead of registering temp table as above)

non_transfer_airports = degree_ratio.join(airports, degree_ratio.id == airports.IATA).selectExpr('id', 'city', 'degree_ratio').filter('degree_ratio < .9 or degree_ratio > 1.1')

# COMMAND ----------

display(non_transfer_airports)

# COMMAND ----------

transfer_airports = degree_ratio.join(airports, degree_ratio.id == airports.IATA).selectExpr('id', 'city', 'degree_ratio').filter('degree_ratio between 0.9 and 1.1')

# COMMAND ----------

display(transfer_airports.limit(10))

# COMMAND ----------

# Breadth First Search 
# Breadth-first search (BFS) is designed to traverse the graph to quickly find the desired vertices (i.e. airports) and edges (i.e flights).  Let's try to find the shortest number of connections between cities based on the dataset.  Note, these examples do not take into account of time or distance, just hops between cities.


# COMMAND ----------

# Example 1: Direct Seattle to San Francisco 
filtered_paths = trip_graph.bfs(fromExpr = "id = 'SEA'", toExpr = "id = 'SFO'", maxPathLength = 1)
display(filtered_paths)

# COMMAND ----------

# Example 1: Direct San Francisco to Bufalo
filtered_paths = trip_graph.bfs(fromExpr = "id = 'SFO'", toExpr = "id = 'BUF'", maxPathLength = 1)
display(filtered_paths)

# COMMAND ----------

filtered_paths = trip_graph.bfs(fromExpr = "id = 'SFO'", toExpr = "id = 'BUF'", maxPathLength = 2)
display(filtered_paths)

# COMMAND ----------

# MAGIC %scala
# MAGIC package d3a
# MAGIC // We use a package object so that we can define top level classes like Edge that need to be used in other cells
# MAGIC
# MAGIC import org.apache.spark.sql._
# MAGIC import com.databricks.backend.daemon.driver.EnhancedRDDFunctions.displayHTML
# MAGIC
# MAGIC case class Edge(src: String, dest: String, count: Long)
# MAGIC
# MAGIC case class Node(name: String)
# MAGIC case class Link(source: Int, target: Int, value: Long)
# MAGIC case class Graph(nodes: Seq[Node], links: Seq[Link])
# MAGIC
# MAGIC object graphs {
# MAGIC val sqlContext = SQLContext.getOrCreate(org.apache.spark.SparkContext.getOrCreate())
# MAGIC import sqlContext.implicits._
# MAGIC
# MAGIC def force(clicks: Dataset[Edge], height: Int = 100, width: Int = 960): Unit = {
# MAGIC   val data = clicks.collect()
# MAGIC   val nodes = (data.map(_.src) ++ data.map(_.dest)).map(_.replaceAll("_", " ")).toSet.toSeq.map(Node)
# MAGIC   val links = data.map { t =>
# MAGIC     Link(nodes.indexWhere(_.name == t.src.replaceAll("_", " ")), nodes.indexWhere(_.name == t.dest.replaceAll("_", " ")), t.count / 20 + 1)
# MAGIC   }
# MAGIC   showGraph(height, width, Seq(Graph(nodes, links)).toDF().toJSON.first())
# MAGIC }
# MAGIC
# MAGIC /**
# MAGIC  * Displays a force directed graph using d3
# MAGIC  * input: {"nodes": [{"name": "..."}], "links": [{"source": 1, "target": 2, "value": 0}]}
# MAGIC  */
# MAGIC def showGraph(height: Int, width: Int, graph: String): Unit = {
# MAGIC
# MAGIC displayHTML(s"""
# MAGIC
# MAGIC   
# MAGIC     
# MAGIC     
# MAGIC   
# MAGIC   
# MAGIC     
# MAGIC     
# MAGIC     
# MAGIC     
# MAGIC     
# MAGIC   
# MAGIC """)
# MAGIC   }
# MAGIC
# MAGIC   def help() = {
# MAGIC displayHTML("""
# MAGIC
# MAGIC Produces a force-directed graph given a collection of edges of the following form:
# MAGIC case class Edge(src: String, dest: String, count: Long)
# MAGIC
# MAGIC Usage:
# MAGIC %scala
# MAGIC import d3._
# MAGIC graphs.force(
# MAGIC   height = 500,
# MAGIC   width = 500,
# MAGIC   clicks: Dataset[Edge])
# MAGIC """)
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala 
# MAGIC package d3a
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC d3a.graphs.help()

# COMMAND ----------

# MAGIC %scala
# MAGIC // On-time and Early Arrivals
# MAGIC import d3a._
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departure_delays_geo where delay <= 0 group by src, dst""").as[Edge])

# COMMAND ----------

departure_delays_geo.createOrReplaceTempView('departure_delays_geo')
departure_delays_geo.cache()

# COMMAND ----------

# MAGIC %scala
# MAGIC // Delayed Trips from CA, OR, and/or WA
# MAGIC import d3a._
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departure_delays_geo where state_src in ('CA', 'OR', 'WA') and delay > 0 group by src, dst""").as[Edge])
# MAGIC      

# COMMAND ----------

sql("""select src, dst as dest, count(1) as count from departure_delays_geo where state_src in ('CA', 'OR', 'WA') and delay > 0 group by src, dst""").display()

# COMMAND ----------


