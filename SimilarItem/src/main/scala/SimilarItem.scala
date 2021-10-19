package com.similaritem

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, IntegerType, LongType, StructType, ArrayType}
import org.apache.spark.sql.DataFrame

object SimilarItem {

  def flatten_df(df: DataFrame): DataFrame = {

    var column_list = List.empty[String]

    for (column_name <- df.schema.names) {
      if (df.schema(column_name).dataType.isInstanceOf[StructType]) {
        for (field <- df.schema(column_name).dataType.asInstanceOf[StructType].fields) {
          column_list:+= column_name + "." + field.name
        }
      }
      else {
        column_list:+= column_name
      }
    }

    val column_flat = column_list.map(name => col(name).alias(name.replace("-", "").replace(".", "_")))

    df.select(column_flat:_*)

  }

  def main(args: Array[String]): Unit = {

    //Print only error
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Create spark session - entry point of the program
    val spark = SparkSession
      .builder()
      .appName("SimilarItem")
      .master("local[4]")
      .getOrCreate()

    //Parse arguments
    val input_file = args(0)
    val output_file = args(1)
    val item_id = args(2)

    //Read data from json input file
    var df = spark.read.json(input_file)

    //Flatten the schema of dataframe from the nested schema
    df = flatten_df(df)

    df.createOrReplaceTempView("Products")

    //Create a dataframe contain the information of given item
    var item_df = spark.sql(s"SELECT * FROM Products WHERE sku='$item_id'")

    import spark.implicits._

    //List of compared sql statement to change similar attr to 1, otherwise 0
    var compare_attr = List.empty[String]
    //List column name from flatten schema
    var column_name = List.empty[String]
    //List weights to evaluate results
    var column_weights = List.empty[String]


    for (column_attr <- item_df.schema) {
      if (column_attr.name != "sku") {
        val column_id = column_attr.name
        var value = item_df.select(column_attr.name).first.getString(0)
        column_name:+= column_id
        compare_attr:+= s"CASE WHEN $column_id='$value' THEN 1 ELSE 0 END AS $column_id"
      }
    }

    var power = column_name.length
    for (attribute <- column_name) {
      column_weights:+= s"$attribute*POWER(2,$power)"
      power -= 1
    }

    //SQL command to change similar attribute as 1, else as 0
    var sql_compare = compare_attr.mkString(", ")

    //SQL command to calculate weights by att_a * 2^n + att_b * 2^(n-1) + ...
    var sql_weights = column_weights.mkString(" + ")
    sql_weights += "AS similarity_weight"

    //SQL command to count number of similar attribute
    var sql_count = column_name.mkString(" + ")
    sql_count += " AS similarity_score"

    df = spark.sql(s"SELECT $sql_compare, sku FROM Products WHERE sku != '$item_id'")
//    df.show(10)

    for (attribute <- column_name) {
      df.withColumn(s"$attribute", col(s"$attribute").cast("int"))
    }
//    df.printSchema()

    df.createOrReplaceTempView("CastProducts")
    df = spark.sql(s"SELECT $sql_count, $sql_weights, sku FROM CastProducts ORDER BY similarity_score DESC, similarity_weight DESC LIMIT 10")
//    df.show(10)
    //Save result to file
    df.coalesce(1).write.option("header","true").csv(output_file)

    spark.stop()

  }

}