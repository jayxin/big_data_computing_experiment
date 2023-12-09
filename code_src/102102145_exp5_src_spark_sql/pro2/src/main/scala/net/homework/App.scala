package net.homework

import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{desc, expr}

import java.nio.file.Paths

object App {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession.builder().appName("pro2").getOrCreate()

      val cwd = Paths.get("").toAbsolutePath.toString
      val inputPath = s"file://${cwd}/input"

      val df = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .format("csv")
        .load(s"${inputPath}/Coffee_Chain.csv")
      df.cache()

      //df.printSchema()

      // 查看咖啡连锁店的销售量排名，按照销售量降序排列。
      println("查看咖啡连锁店的销售量排名，按照销售量降序排列")
      df.select("product", "marketing")
        .groupBy("product")
        .agg(expr("SUM(marketing) AS number"))
        .orderBy(desc("number"))
        .show()
      println("======")

      // 查看咖啡销售量和所在州的关系，按降序排列。
      println("查看咖啡销售量和所在州的关系，按降序排列")
      df.select("state", "marketing")
        .groupBy("state")
        .agg(expr("SUM(marketing) AS number"))
        .orderBy(desc("number"))
        .show()
      println("======")

      // 查询咖啡的平均利润和售价，按平均利润降序排列。
      println("查询咖啡的平均利润和售价，按平均利润降序排列")
      df.select("product", "coffee sales", "profit")
        .groupBy("product")
        .agg(expr("avg(`coffee sales`)"), expr("avg(profit) as avg_profit"))
        .orderBy(desc("avg_profit"))
        .show()
      println("======")

      // 查询市场规模、市场地域与销售量的关系。按总销量降序排列.
      println("查询市场规模、市场地域与销售量的关系。按总销量降序排列")
      df.select("market", "market size", "coffee sales")
        .groupBy("market", "market size")
        .agg(expr("SUM(`coffee sales`) AS total_sales"))
        .orderBy(desc("total_sales"))
        .show()
      println("======")

      // 查询咖啡属性与平均售价、平均利润、销售量与其他成本的关系。
      println("查询咖啡属性与平均售价、平均利润、销售量与其他成本的关系")
      df.select("type", "profit", "coffee sales",  "total expenses",
        "marketing", "budget sales")
        .groupBy("type")
        .agg(
          expr("AVG(`coffee sales`) AS avg_sales"),
          expr("AVG(profit) AS avg_profit"),
          expr("SUM(`coffee sales`) AS total_sales"),
          expr("AVG(`total expenses`) AS other_cost"))
        .show()
      println("======")

      spark.stop()
  }
}
