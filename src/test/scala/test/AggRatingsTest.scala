package test

import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneId}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

class AggRatingsTest extends FunSuiteLike with Matchers with BeforeAndAfterAll {

  val cpuCount = 4

  val spark =
    SparkSession.builder()
      .master(s"local[$cpuCount]")
      .appName(this.getClass.getSimpleName)
      .config("spark.ui.enabled", value = false)
      .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = spark.stop()

  val now = LocalDateTime.now()
  val nowTs = toTimestamp(now)
  val oneDayBeforeTs = toTimestamp(now.minus(1, ChronoUnit.DAYS))

  test("should produce agg_ratings df") {
    val data =
      Seq(
        ("u1", "p1", 1.0, nowTs),
        ("u1", "p1", 1.0, oneDayBeforeTs),
        ("u1", "p2", 2.0, nowTs),
        ("u2", "p3", 3.0, nowTs),
        ("u4", "p4", 0.01, nowTs)
      ).toDF("userId,productId,rating,timestamp".split(","): _*)

    val userLookup =
      Seq(
        "u1" -> 0L,
        "u2" -> 1L,
        "u4" -> 2L
      ).toDF("userId", "userIdAsInteger")

    val productLookup =
      Seq(
        "p1" -> 0L,
        "p2" -> 1L,
        "p3" -> 2L,
        "p4" -> 3L
      ).toDF("productId", "productIdAsInteger")

    val result: DataFrame = AggRatings.produceAggRatings(data, userLookup, productLookup)

    result.columns should (
      have size 3
        and contain allOf("userIdAsInteger", "productIdAsInteger", "ratingSum")
      )

    val aggRatings =
      result.select("userIdAsInteger", "productIdAsInteger", "ratingSum")
        .collect()
        .map { case Row(userIdAsInteger: Long, productIdAsInteger: Long, ratingSum: Double) =>
          (userIdAsInteger, productIdAsInteger, ratingSum)
        }

    aggRatings should (
      have size 3
        and contain allOf(
        (0, 0, 1.95),
        (0, 1, 2.0),
        (1, 2, 3.0)
      )
      )
  }

  test("should get 0 when no day spent") {
    AggRatings.daysSpent(nowTs, nowTs) should be(0L)
  }

  test("should get 1 when one day spent") {
    AggRatings.daysSpent(nowTs, oneDayBeforeTs) should be(1L)
  }

  test("should apply penalty of 1.0 when no day spent") {
    AggRatings.penalty(0L) should be(1.0 +- 1e-7)
  }

  test("should apply penalty of 0.95 when one day spent") {
    AggRatings.penalty(1L) should be(0.95 +- 1e-7)
  }

  test("shoud get max timestamp") {
    val data =
      Seq(1L, 2L, 5L, 4L).toDF("timestamp")

    AggRatings.maxTimestamp(data) should be(5L)
  }

  test("should apply penalty to ratings DF according to days spent") {
    val data =
      Seq(
        (1.0, nowTs),
        (1.0, oneDayBeforeTs)
      ).toDF("rating", "timestamp")

    val result: DataFrame = AggRatings.applyPenalty(data, nowTs)

    val resultData =
      result.select("updatedRating")
        .collect()
        .map { case Row(updatedRating: Double) => updatedRating }

    resultData should (
      have size 2
        and contain allOf(1.0, 0.95)
      )
  }

  test("should filter ratings > 0.01") {
    val data =
      Seq(1.0, 0.01).toDF("updatedRating")

    val result: DataFrame = AggRatings.filterRatings(data)

    val resultData =
      result.select("filteredRating")
        .collect()
        .map { case Row(filteredRating: Double) => filteredRating }

    resultData should (
      have size 1
        and contain(1.0)
      )
  }

  test("should group and sum ratings") {
    val data =
      Seq(
        ("u1", "p1", 1.0),
        ("u1", "p1", 1.0),
        ("u1", "p2", 1.0),
        ("u2", "p1", 1.0),
        ("u3", "p3", 1.0)
      ).toDF("userId", "productId", "filteredRating")

    val result: DataFrame = AggRatings.groupAndSumRatings(data)

    val resultData =
      result.select("userId", "productId", "ratingSum")
        .collect()
        .map { case Row(userId: String, productId: String, ratingSum: Double) => (userId, productId, ratingSum) }

    resultData should (
      have size 4
        and contain allOf(
        ("u1", "p1", 2.0),
        ("u1", "p2", 1.0),
        ("u2", "p1", 1.0),
        ("u3", "p3", 1.0)
      )
      )
  }

  test("should produce lookup DF") {
    val data =
      Seq("wlkfn", "lwekfw", "e029ru").toDF("id")

    val result: DataFrame = AggRatings.produceLookup(data, "id", spark)

    val resultData =
      result.select("idAsInteger")
        .collect()
        .map { case Row(idAsInteger: Long) => idAsInteger }

    resultData should (
      have size 3
        and contain allOf(0, 1, 2)
      )
  }

  test("should produce user lookup DF") {
    val data =
      Seq("u1").toDF("userId")

    val result: DataFrame = AggRatings.produceUserLookup(data, spark)

    val resultData =
      result.select("userIdAsInteger")
        .collect()
        .map { case Row(userIdAsInteger: Long) => userIdAsInteger }

    resultData should (
      have size 1
        and contain(0)
      )
  }

  test("should produce product lookup DF") {
    val data =
      Seq("u1").toDF("productId")

    val result: DataFrame = AggRatings.produceProductLookup(data, spark)

    val resultData =
      result.select("productIdAsInteger")
        .collect()
        .map { case Row(productIdAsInteger: Long) => productIdAsInteger }

    resultData should (
      have size 1
        and contain(0)
      )
  }

  test("should anonymize userId and productId") {
    val data =
      Seq(
        ("u1", "p1"),
        ("u1", "p2"),
        ("u2", "p1"),
        ("u3", "p3")
      ).toDF("userId", "productId")

    val userLookup =
      Seq(
        "u1" -> 0L,
        "u2" -> 1L,
        "u3" -> 2L
      ).toDF("userId", "userIdAsInteger")

    val productLookup =
      Seq(
        "p1" -> 0L,
        "p2" -> 1L,
        "p3" -> 2L
      ).toDF("productId", "productIdAsInteger")

    val result: DataFrame = AggRatings.anonymize(data, userLookup, productLookup)

    result.columns should (
      have size 2
        and contain allOf("userIdAsInteger", "productIdAsInteger")
      )

    val resultData: Array[(Long, Long)] =
      result.select("userIdAsInteger", "productIdAsInteger")
        .collect()
        .map { case Row(userIdAsInteger: Long, productIdAsInteger: Long) =>
          (userIdAsInteger, productIdAsInteger)
        }

    resultData should (
      have size 4
        and contain allOf(
        (0, 0),
        (0, 1),
        (1, 0),
        (2, 2)
      ))

  }

  private def toTimestamp(now: LocalDateTime) = {
    now.atZone(ZoneId.of("UTC")).toInstant.toEpochMilli
  }

}
