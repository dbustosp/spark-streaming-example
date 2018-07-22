package com.streaming.example

import java.io.File

import com.fasterxml.jackson.databind.deser.std.StringDeserializer
import com.typesafe.config.{Config, ConfigFactory}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j
import org.kohsuke.args4j.{CmdLineException, CmdLineParser}


/**
  * Created by danilobustos on 22-07-18.
  */
object StreamingExecutor {
  @args4j.Option(name = "--configFile", required = true, usage = "Configuration file for the application")
  var configFile : String = ""
  @args4j.Option(name = "--topic", required = true, usage = "Topic to be consumed")
  var topic : String = ""
  @args4j.Option(name = "--output", required = true, usage = "Output Path")
  var outputPath : String = ""

  var sqlContext : SQLContext = null


  def main(args : Array[String]) = {

    val parser = new CmdLineParser(this)
    try {
      parser.parseArgument(scala.collection.JavaConversions.seqAsJavaList(args))
    } catch {
      case e : CmdLineException => {
        throw new IllegalArgumentException("Invalid arguments provided", e)
      }
    }

    val config = ConfigFactory.parseFile(new File(configFile))

    val zkUrlClient = config.getString("zookeeper.client.host") + ":" + config.getString("zookeeper.client.port")
    val zkUrlServer = config.getString("zookeeper.server.host") + ":" + config.getString("zookeeper.server.port")
    val groupId = config.getString("kafka.consumers.groupid")
    val windowBatch = config.getInt("spark.streaming.batchduration.seconds")

    val conf = new SparkConf()
      .set("spark.streaming.backpressure.initialRate", "100")
      .set("spark.streaming.stopGracefullyOnShutDown", "false")
      .setAppName("Spark Streaming Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(windowBatch))
    val kafkaParams = getKafkaParams(config)

    val dStream = KafkaUtils.createDirectStream[String, GenericRecord](ssc, PreferConsistent, Subscribe[String, GenericRecord](Array(topic), kafkaParams))

    dStream.foreachRDD { rdd =>
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println("off: " + offsetRanges)
      }
    }
  }

  def getKafkaParams(config : Config) : Map[String, Object] = {
    val zkURL = config.getString("zookeeper.client.host") + ":" + config.getString("zookeeper.client.port")
    val params = Map[String, Object](
      "bootstrap.servers" -> config.getString("kafka.brokers"),
      "enable.auto.commit" -> (false : java.lang.Boolean),
      "auto.offset.reset" -> config.getString("kafka.consumers.strategy"),
      "group.id" -> config.getString("kafka.consumers.groupid"),
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      "zookeeper.connect" -> zkURL,
      "zookeeper.connection.timeout.ms" -> "2000",
      "consumer.timeout.ms" -> "20000",
      "request.timeout.ms" -> "80000",
      "schema.registry.url" -> config.getString("kafka.schema.registry.url")

    )
    // Returning Kafka Params
    params
  }

}
