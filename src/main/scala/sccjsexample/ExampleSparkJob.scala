package sccjsexample

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import spark.jobserver.{SparkJob, SparkJobValid, _}
import com.typesafe.config.Config


object ExampleSparkJob extends SparkJob {

  case class KV(k: Int, v: Int)

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val con = CassandraConnector(sc.getConf)
    con.withSessionDo({ session =>
      session.execute(
        """DROP KEYSPACE IF EXISTS test"""
      )

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

    //dse hadoop fs -mkdir /test_spark_sql_cfs
    //dse hadoop fs -copyFromLocal example.txt /test_spark_sql_cfs/
    val textFile = sc.textFile("/test_spark_sql_cfs/example.txt")
    val data = textFile.map(line => line.split(" ").map(elem => elem.trim).map(elem => elem.toInt))
    val dataBy1 = data.map(row => (row(0), row(1))).keyBy(row => row._1)
    val kvByk = sc.cassandraTable[KV]("test", "kv").keyBy(kv => kv.k)
    dataBy1.join(kvByk).collect()
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    return SparkJobValid
  }
}
