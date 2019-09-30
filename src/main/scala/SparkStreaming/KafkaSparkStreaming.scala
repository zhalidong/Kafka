package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by zld on 2019/9/30 0030.
  *
  *通过SparkStreaming从Kafka读取数据，
  * 并将读取过来的数据做简单计算(WordCount)，最终打印到控制台
  *
  *
  */
object KafkaSparkStreaming {

  def main(args: Array[String]): Unit = {
	//1.创建SparkConf并初始化SSC
	val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")
	val ssc = new StreamingContext(sparkConf, Seconds(5))


	//4.通过KafkaUtil创建kafkaDSteam
	val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
	  ssc, "hadoop-01:2181", "atguigu", Map("atguigu" -> 3)
	)
	val wordDStream: DStream[String] = kafkaDstream.flatMap(t=>t._2.split(" "))

	val mapDstream: DStream[(String, Int)] = wordDStream.map((_,1))

	val wordtosumdstream: DStream[(String, Int)] = mapDstream.reduceByKey(_+_)

	wordtosumdstream.print()

	//6.启动SparkStreaming
	ssc.start()
	ssc.awaitTermination()
  }
}