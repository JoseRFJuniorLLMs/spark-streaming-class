import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._

object Main {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-kafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(4))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(0),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test"
    )


    val topics = Array(args(1))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.map(record => (record.key, record.value))

    stream.foreachRDD(rdd =>
      println(s"*************###########*******************${rdd.count}*************##########*******************")
    )
    ssc.start()
    ssc.awaitTermination()
  }

}


