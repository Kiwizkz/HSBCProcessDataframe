package com.kiwi.app
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test
class HSBCProcessDataframeAppTest {
  object SparkSessionUtils {
    def createSparkSession(appName: String): SparkSession = {
      SparkSession.builder()
        .appName(appName)
        .master("local[*]")
        .getOrCreate()
    }
  }

  //Unit test data
  val testData: Seq[(String, String, String, Int)] = Seq(
    ("AE686(AE)", "7", "AE686", 2022),
    ("AE686(AE)", "8", "BH2740", 2021),
    ("AE686(AE)", "9", "EG999", 2021),
    ("AE686(AE)", "10", "AE0908", 2023),
    ("AE686(AE)", "11", "QA402", 2022),
    ("AE686(AE)", "12", "OA691", 2022),
    ("AE686(AE)", "12", "OB691", 2022),
    ("AE686(AE)", "12", "OC691", 2019),
    ("AE686(AE)", "12", "OD691", 2017)
  )

  def assertDataFrameEquals(df1: DataFrame, df2: DataFrame): Unit = {
    assert(df1.collect().sameElements(df2.collect()))
  }

  //first Unit test:the threshold_test = 5
  @Test
  def ProcessDataframeTest(): Unit = {
    // 创建 SparkSession
    val spark = SparkSessionUtils.createSparkSession("HSBCProcessDataframeTest")

    import spark.implicits._

    val df_test = testData.toDF("peer_id", "id_1", "id_2", "year")
    val threshold_test = 5

    val result1 = HSBCProcessDataframeApp.processDataFrame(df_test, threshold_test)

    val expected = Seq(
      ("AE686(AE)", 2022),
      ("AE686(AE)", 2021)).toDF("peer_id", "year")

    assertDataFrameEquals(result1, expected)

    spark.stop()
  }

  //Second Unit test:the threshold_test = 7
  @Test
  def ProcessAnotherDataframeTest(): Unit = {

    // 创建 SparkSession
    val spark = SparkSessionUtils.createSparkSession("HSBCProcessDataframeTest")

    import spark.implicits._

    val df_test = testData.toDF("peer_id", "id_1", "id_2", "year")
    val threshold_test2 = 7

    val result2 = HSBCProcessDataframeApp.processDataFrame(df_test, threshold_test2)
    val expected2 = Seq(
      ("AE686(AE)", 2022),
      ("AE686(AE)", 2021),
      ("AE686(AE)", 2019)).toDF("peer_id", "year")

    assertDataFrameEquals(result2, expected2)

    spark.stop()
  }


}
