package sccjsexample

import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobValid, _}
import com.typesafe.config.Config

object ExampleSparkCFSJSONJob extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {

    //dse hadoop fs -mkdir /test_spark_sql_cfs
    //dse hadoop fs -copyFromLocal people.json /test_spark_sql_cfs/
    val sqlc = new org.apache.spark.sql.SQLContext(sc)
    import sqlc._
    import sqlc.implicits._
    val inpath = "cfs:///test_spark_sql_cfs/people.json"
    val cfs_people = sqlc.jsonFile(inpath)
    cfs_people.printSchema()
    cfs_people.registerTempTable("cfs_people")
    val cfs_teenagers = sqlc.sql("SELECT name FROM cfs_people WHERE age >= 13 AND age <= 19")
    cfs_teenagers.collect()
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    return SparkJobValid
  }
}
