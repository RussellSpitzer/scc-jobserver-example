package sccjsexample

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJob, SparkJobValid, _}
import com.typesafe.config.Config


object ExampleSparkJob extends SparkJob {
  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val con = CassandraConnector(sc.getConf)
    con.withSessionDo({ session =>
      session.execute(
        """CREATE KEYSPACE IF NOT EXISTS test
          |WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }""".stripMargin)

      session.execute(
        """CREATE TABLE IF NOT EXISTS test.kv (k int PRIMARY KEY, v int)"""
      )
      session.execute(
        """CREATE TABLE IF NOT EXISTS test.copykv (k int PRIMARY KEY, v int)"""
      )
      for (i <- 1 to 1000) {
        session.execute("INSERT INTO test.kv (k, v) VALUES (?,?)", i:java.lang.Integer, i:java.lang.Integer)
      }
    })

    sc.cassandraTable("test", "kv").saveToCassandra("test", "copykv")
    sc.cassandraTable("test", "copykv").take(10)

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
    cfs_teenagers.collect().foreach(println)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    return SparkJobValid
  }
}
