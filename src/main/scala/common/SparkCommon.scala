package common


import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import org.slf4j.LoggerFactory

object SparkCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(): Option[SparkSession] = {
    try {
      
	  // Create a Spark Session
      logger.info("createSparkSession() started")
	  System.setProperty("hadoop.home.dir", "C:\\winutils")
	  
      val spark = SparkSession
        .builder()
        .appName("HelloSpark")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      println("Created Spark Session")
      
	  
	  val sampleSeq = Seq((1,"spark"),(2,"Big Data"))
      val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
      df.show()
	  
	  
      // returns the Spark Session
      logger.info("Returning the Spark Session")
      Some(spark)
    } catch {

      case e: Exception =>
        logger.error("An error has occured in Spark session creation method "+e.printStackTrace())
        System.exit(1)
        None
    }

  }
  
  }