package com.kiwi.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object HSBCProcessDataframeApp {
  def processDataFrame(df: DataFrame, threshold: Int): DataFrame = {

    // Step1: For each peer_id, get the year when peer_id contains id_2
    val baseYearDF = df.filter(col("peer_id").contains(col("id_2"))).select(col("peer_id"), col("year").alias("year_limit"))
    println("get the year when peer_id contains id_2:")
    baseYearDF.show()

    val joinedBaseYearDF = df.join(baseYearDF, "peer_id")

    // Step2: For each peer_id count the number of each year which is smaller than or equal to the year in step1
    val yearCountDF = joinedBaseYearDF.filter("year <= year_limit").
      groupBy("peer_id", "year").count().
      orderBy(col("peer_id"), col("year").desc)
    println("count the number of each year which is smaller than or equal to the year in step1:")
    yearCountDF.show()

    // Create a window function rule for cumulative sum.
    val windowSpec = Window.partitionBy("peer_id").orderBy(col("year").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Step3: Calculate the cumulative sum of the count of years.
    val cumulative_sum_DF = yearCountDF.withColumn("cumulative_sum", sum("count").over(windowSpec))
    println("Calculate the cumulative sum of the count of years:")
    cumulative_sum_DF.show()

    //Step4: Calculate the minimum cumulative sum which is greater than or equal to a threshold.
    val min_cumulative_sum_dataframe = cumulative_sum_DF.
      filter(col("cumulative_sum") >= threshold).
      groupBy(col("peer_id")).
      agg(min(col("cumulative_sum")).
        alias("min_cumulative_sum"))
    println("Calculate the minimum cumulative sum which is greater than or equal to a threshold:")
    min_cumulative_sum_dataframe.show()

    //Step5: join the cumulative_sum_dataframe and min_cumulative_sum_dataframe to get result whose
    // cumulative sum of the count of years is less than or equal to the minimum cumulative value.
    val resultDF = cumulative_sum_DF.join(min_cumulative_sum_dataframe, "peer_id").
      filter("cumulative_sum <= min_cumulative_sum").
      select(col("peer_id"), col("year")).
      orderBy(col("peer_id"), col("year").desc)

    println("the result:")
    //return the result Dataframe
    resultDF

  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("hsbcProcessDF")
      .master("local[*]")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // 读取数据并创建DataFrame
    val testDF = Seq(
      ("ABC17969(AB)", "1", "ABC17969", 2022),
      ("ABC17969(AB)", "2", "CDC52533", 2022),
      ("ABC17969(AB)", "3", "DEC59161", 2023),
      ("ABC17969(AB)", "4", "F43874", 2022),
      ("ABC17969(AB)", "5", "MY06154", 2021),
      ("ABC17969(AB)", "6", "MY4387", 2022),
      ("AE686(AE)", "7", "AE686", 2023),
      ("AE686(AE)", "8", "BH2740", 2021),
      ("AE686(AE)", "9", "EG999", 2021),
      ("AE686(AE)", "10", "AE0908", 2021),
      ("AE686(AE)", "11", "QA402", 2022),
      ("AE686(AE)", "12", "OM691", 2022)
    )
    val df = spark.createDataFrame(testDF).toDF("peer_id", "id_1", "id_2", "year")
    val threshold = 3
    val resultDF = processDataFrame(df, threshold)
    resultDF.show()
    spark.stop()
  }
}
