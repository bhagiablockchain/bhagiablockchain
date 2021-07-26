

import common.{PostgresCommon, SparkCommon}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object ScalaLoggingMain {

  private val logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      logger.info("main method started")
      logger.warn("This is a warning")
      

      

      logger.warn("Calling CreateSparkSession")
      val spark = SparkCommon.createSparkSession().get



      val PostgresDF = PostgresCommon.fetchDataFrameFromPgTable(spark).get
      PostgresDF.show()
	  logger.info("All Records Fetched....")

    } catch {
      case e:Exception =>
        logger.error("An error has occured in the main method "+ e.printStackTrace())
    }
  }


}
