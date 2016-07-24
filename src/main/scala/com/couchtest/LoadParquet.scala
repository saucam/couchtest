package com.couchtest

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.spark._

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ydatta on 7/24/2016 AD.
  */
object LoadParquet {

  def createKey(uniqueId: String, timestamp: String): String = uniqueId + timestamp

  def loadParquet(): Unit = {
    val input = "/Users/ydatta/Documents/Workspace/data/new_data/schema"
    val output = "parquetbucket"

    val sparkConf = new SparkConf()
      .setAppName("CouchParquetTest")
      .setMaster("local[*]")
      .set("spark.executor.memory", "6g")
      .set("spark.ui.port", "8099")
      // connect to grocery bucket
      .set(s"com.couchbase.bucket.${output}", "")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    // Making 1 partition because we get backpressure exception from couchbase
    val parquetData = sqlContext.read.parquet(input)

    parquetData.registerTempTable("schema")
    //val k = sqlContext.sql("select count (distinct unique_id) from schema").collect
    // Print schema
    parquetData.printSchema

    val jsonRDD = parquetData.toJSON
    val jsonObjectRDD = jsonRDD.map(content => JsonObject.fromJson(content))
    val jsonDocumentRDD = jsonObjectRDD.map(obj =>
      JsonDocument.create(
        createKey(obj.get("timestamp").toString,obj.get("unique_id").toString) , obj))

    jsonDocumentRDD.coalesce(1, false)
    jsonDocumentRDD.saveToCouchbase(output)

  }

}
