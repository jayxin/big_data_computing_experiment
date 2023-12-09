package net.homework;

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, col, row_number}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{
  StructField, StructType,
  StringType, LongType, DoubleType
}
import org.apache.spark.sql.Row

import java.nio.file.Paths

object App {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .appName("pro1")
      .getOrCreate()

      val cwd = Paths.get("").toAbsolutePath.toString
      val inputPath = s"file://${cwd}/input"
      val outputPath = s"file://${cwd}/output"

      val schema = new StructType(Array(
        new StructField("DATE", StringType, true),
        new StructField("USERID", StringType, true),
        new StructField("ITEM", StringType, true),
        new StructField("CITY", StringType, true),
        new StructField("PLATFORM", StringType, true)
      ))

      val df = spark.read
        .option("header", "false")
        .option("sep", "\t")
        .schema(schema)
        .format("csv")
        .load(s"${inputPath}/UserlogsHot.log")
      df.cache()

      // 查询每天每用户点击某商品的次数, 输出到屏幕.
      val query1 = df.groupBy("USERID", "DATE", "ITEM")
        .agg(expr("COUNT(*) AS CNT"))

      query1.cache()

      query1.collect()
        .foreach(println)

      // 将用户每天点击商品的次数汇总累加,
      // 统计每用户在每天点击搜索每个商品的总次数,
      // 输出 JSON 格式文件, JSON 元数据格式如下:
      // Date(日期)、UserID(用户 id)、Item(商品)、Count(该用户在当天点击该商品的次数)
      query1.write
        .format("json")
        .mode("overwrite")
        .save(s"${outputPath}/query1.json")

      // 统计出用户搜索每商品的前 $ 3 $ 名:
      // 用步骤 $ 2 $ 的 JSON 字符串, 构造DataFrame。
      // 在 Spark SQL 注册临时表, 使用窗口函数 row\_number 统计出每用户搜索每商品的前
      // $ 3 $ 名, 将结果以 JSON 格式输出到屏幕或者文件。
      val df1 = spark.read
        .option("inferSchema", "true")
        .format("json")
        .load(s"${outputPath}/query1.json")

      df1.createOrReplaceTempView("query1_view")
      df1.show()

      val windowSpec = Window.partitionBy("USERID")
        .orderBy(col("CNT").desc)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

      val c = row_number().over(windowSpec)
      df1.select(col("Date"), col("USERID"), col("ITEM"), col("CNT")).show()

      spark.stop()
  }
}
