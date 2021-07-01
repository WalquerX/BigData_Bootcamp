package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercise.streaming.StreamingJob2.{computeDevicesCountByCoordinates, enrichAntennaWithMetadata, spark}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, lit, max, min, sum, when, window}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object BatchJob2 extends BatchJob {
  override val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Streaming Job")
      .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where($"year" === filterDate.getYear &&
        $"month" === filterDate.getMonthValue&&
        $"day" === filterDate.getDayOfMonth &&
        $"hour" === filterDate.getHour
      )

  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read                //solo read porque los datos son estáticos
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF
      .join(metadataDF,
        antennaDF("id") === metadataDF("id")
      ).drop(metadataDF("id")) //se quita una de las columnas id
  }

  def computeDevicesCountByCoordinates(dataFrame: DataFrame, aggColumn: String): DataFrame = {
    dataFrame
      .select($"timestamp".cast(TimestampType).as("timestamp"), col(aggColumn), $"bytes")
      .groupBy(col(aggColumn), window($"timestamp", "1 hour"))
      .agg(sum($"bytes").as("total_bytes"))
      .select(
        $"window.start".cast(TimestampType).as("timestamp"),
        col(aggColumn).as("id"),
        $"total_bytes".as("value"),
        lit(s"${aggColumn}_total_bytes").as("type")
      )
  }

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???



  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }



  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .save(s"$storageRootPath/cold-historical")
  }

  def main(args: Array[String]): Unit = {
    val argsTime = "2021-07-01T16:00:00Z"
    val storageDF = readFromStorage("/tmp1/practica/data", OffsetDateTime.parse(argsTime))
    val metaDataDF = readAntennaMetadata("jdbc:postgresql://35.199.112.5:5432/postgres","user_metadata","postgres","keepcoding")

    val antenna_byte_sum = computeDevicesCountByCoordinates(enrichAntennaWithMetadata(storageDF, metaDataDF), "antenna_id")
    val email_byte_sum = computeDevicesCountByCoordinates(enrichAntennaWithMetadata (storageDF, metaDataDF), "email")
    val app_byte_sum = computeDevicesCountByCoordinates(enrichAntennaWithMetadata(storageDF, metaDataDF), "app")

    val user_byte_sum = computeDevicesCountByCoordinates(storageDF, "id")
    val userQuotaLimitDF = user_byte_sum.as("user").select($"id", $"value", $"timestamp")
      .join(
        metaDataDF.select($"id", $"email", $"quota").as("metadata"),
        $"user.id" === $"metadata.id" && $"user.value" > $"metadata.quota"
      ).select($"metadata.email", $"user.value".as("usage"), $"metadata.quota", $"timestamp".as("timestamp"))


    writeToJdbc(antenna_byte_sum, "jdbc:postgresql://35.199.112.5:5432/postgres","bytes_hourly","postgres", "keepcoding")
    writeToJdbc(email_byte_sum, "jdbc:postgresql://35.199.112.5:5432/postgres","bytes_hourly","postgres", "keepcoding")
    writeToJdbc(app_byte_sum, "jdbc:postgresql://35.199.112.5:5432/postgres","bytes_hourly","postgres", "keepcoding")
    writeToJdbc(userQuotaLimitDF, "jdbc:postgresql://35.199.112.5:5432/postgres","user_quota_limit","postgres", "keepcoding")


    writeToStorage(storageDF, "/tmp1/practica/")

  }

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = ???
}
