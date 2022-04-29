/*
 * @Descripttion: 
 * @version: 
 * @Author: Heng Tyrion Wang
 * @Date: 2022-02-16 18:01:17
 * @LastEditors: Heng Tyrion Wang
 * @Email: hengtyrionwang@gmail.com
 * @LastEditTime: 2022-02-28 12:55:01
 */
package com.example.data.tfrecord

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, udf, lit, split, rand}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.collection.mutable.Map

object FFMTfrecord {
    
    def getFieldIdx = udf((data : Seq[String]) => data.map(x => x.split(":")(0)).mkString(" "))

    def getFeatureIdx = udf((data : Seq[String]) => data.map(x => x.split(":")(1)).mkString(" "))

    def getFeatureVals = udf((data : Seq[String]) => data.map(x => x.split(":")(2)).mkString(" "))
    
    def getFieldSubIdx = udf((data : Seq[String]) => {
      var results : String = ""
      var dict:Map[String, Int] = Map()
      for ( d <- data ) {
        var items = d.split(":")
        if(dict.contains(items(0))){
          dict += (items(0) -> (dict(items(0))+1))
        } else {
          dict += (items(0) -> 0)
        }
        results = results + " " + dict(items(0)).toString
      }
      results
    })

    def getFieldNum = udf((data: Seq[String]) => data.map(x => x.split(":")(0)).toSet.size)

    def getFeatureNum = udf((data: Seq[String]) => data.map(x => x.split(":")(1)).max)

    def getFieldSubNum = udf((data : Seq[String]) => {
      var results =0
      var dict:Map[String, Int] = Map()
      for (d <- data) {
        var items = d.split(":")
        if(dict.contains(items(0))) {
          dict += (items(0) -> (dict(items(0))+1))
        } else {
          dict += (items(0) -> 0)
        }
      }
      results = dict.values.max
      results+1
    })

    def getData(spark: SparkSession, dataType: String): DataFrame = {
      var sqlString = s"select label, feature from criteo.${dataType}_set"
      var data = spark.sql(sqlString)
      data
    }

    def convert(spark: SparkSession, dataType: String, path: String): Unit = {
      var data = getData(spark, dataType)
      data = data.withColumn("feature_array", split(col("feature"), " ")).drop("feature")
      data = data.withColumn("field_idx", getFieldIdx(col("feature_array")))
      data = data.withColumn("field_sub_idx", getFieldSubIdx(col("feature_array")))
      data = data.withColumn("feature_idx", getFeatureIdx(col("feature_array")))
      data = data.withColumn("feature_vals", getFeatureVals(col("feature_array")))
      if(dataType=="train"){
        var nums = data.withColumn("field_sub_num", getFieldSubNum(col("feature_array")))
        nums = nums.withColumn("field_num", getFieldNum(col("feature_array")))
        nums = nums.withColumn("feature_num", getFeatureNum(col("feature_array")))
        nums = nums.select("field_num", "field_sub_num", "Feature_num").agg(("field_num", "max"), ("field_sub_num", "max"), ("feature_num", "max"))
        nums.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(path + "/conf/conf.csv")
      }
      data = data.drop("feature_array")
      data = data.withColumn("rand", rand()).orderBy("rand").drop("rand")
      data.write.format("tfrecords").mode("overwrite").option("recordType", "Example").save(path + "/tfdata/" + dataType + ".tfrecord")
    }

    def fit(spark: SparkSession, path: String): Unit = {
      convert(spark, "train", path)
      convert(spark, "valid", path)
      convert(spark, "test", path)
    }

    def main(args: Array[String]) {
      var (path) = (args(0))
      val spark = SparkSession.builder().appName("svm tfrecord").enableHiveSupport().getOrCreate()
      fit(spark, path.trim)
    }
}
