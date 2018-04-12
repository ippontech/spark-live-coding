package test

import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object AggRatings {
  val userId = "userId"
  val productId = "productId"
  val timestamp = "timestamp"

  def produceAggRatings(df: DataFrame, userLookup: DataFrame, productLookup: DataFrame): DataFrame = {
    val maxTs = maxTimestamp(df)

    val updatedDf = applyPenalty(df, maxTs)
    val filteredDf = filterRatings(updatedDf)
    val groupedDf = groupAndSumRatings(filteredDf)

    anonymize(groupedDf, userLookup, productLookup)
  }

  def anonymize(df: DataFrame, userLookup: DataFrame, productLookup: DataFrame): DataFrame =
    df
      .join(userLookup, userId)
      .join(productLookup, productId)
      .drop(userId, productId)

  def produceProductLookup(df: DataFrame, spark: SparkSession): DataFrame = produceLookup(df, productId, spark)

  def produceUserLookup(df: DataFrame, spark: SparkSession): DataFrame = produceLookup(df, userId, spark)

  def produceLookup(df: DataFrame, idColumn: String, spark: SparkSession): DataFrame = {
    val rdd =
      df.select(idColumn)
        .distinct()
        .rdd
        .zipWithIndex()
        .map { case (Row(id: String), index: Long) => Row(id, index) }

    spark.createDataFrame(rdd,
      StructType(
        StructField(idColumn, StringType)
          :: StructField(idColumn + "AsInteger", LongType)
          :: Nil))
  }

  def groupAndSumRatings(df: DataFrame): DataFrame =
    df.groupBy(userId, productId)
      .agg(sum("filteredRating").as("ratingSum"))

  def filterRatings(df: DataFrame): DataFrame =
    df.where(col("updatedRating") > 0.01)
      .withColumnRenamed("updatedRating", "filteredRating")

  def applyPenalty(df: DataFrame, maxTimestamp: Long): DataFrame =
    df.withColumn("updatedRating", col("rating") * penaltyUdf(maxTimestamp)(col(timestamp)))

  def maxTimestamp(df: DataFrame): Long =
    df.agg(max(timestamp).as(timestamp))
      .head()
      .getAs[Long](timestamp)

  def penaltyUdf(maxTimestamp: Long): UserDefinedFunction =
    udf[Double, Long](ts => penalty(daysSpent(ts, maxTimestamp)))

  def penalty(daysSpent: Long): Double = Math.pow(0.95, daysSpent.toDouble)

  def daysSpent(ts1: Long, ts2: Long): Long =
    toLocalDateTime(ts1).until(toLocalDateTime(ts2), ChronoUnit.DAYS).abs

  private def toLocalDateTime(ts1: Long) = {
    Instant.ofEpochMilli(ts1).atZone(ZoneId.of("UTC")).toLocalDateTime
  }

}
