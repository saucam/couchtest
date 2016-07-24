package com.couchtest

import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.{JsonDocument, RawJsonDocument}
import com.couchbase.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object LoadTest {
  def main(args: Array[String]): Unit = {

    /*// Path to json data
    var input = new String;
    // Couchbase bucket
    var output = new String;
    if (args.length < 2 ) {
      input = "/Users/ydatta/Documents/Workspace/data/new_data/tmp/grocery-1.json"

      output = "grocery"
    }
    else {
      input = args(0)
      output = args(1)
    }

    val sparkConf = new SparkConf()
      .setAppName("CouchTest")
      .setMaster("local[*]")
      .set("spark.executor.memory", "6g")
      .set("spark.ui.port", "8099")
      // connect to grocery bucket
      .set(s"com.couchbase.bucket.${output}", "")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val jsonData = sqlContext.read.json(input).coalesce(1)
    // Print shcema
    jsonData.printSchema

    //register as temp table
    // jsonData.registerTempTable("jsondata")

    val jsonRDD = jsonData.toJSON
    val jsonObjectRDD = jsonRDD.map(content => JsonObject.fromJson(content))
    val jsonDocumentRDD = jsonObjectRDD.map(obj => JsonDocument.create(obj.get("unique_id").toString, obj))

    //val c = jsonDocumentRDD.take(20)
    jsonDocumentRDD.saveToCouchbase(output)

    val data = sc
      .parallelize(Seq("user_10002", "id2", "id3"))
      .couchbaseGet[JsonDocument]()
      .collect() */

    LoadParquet.loadParquet

  }
}
