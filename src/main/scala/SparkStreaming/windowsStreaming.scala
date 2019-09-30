package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by zld on 2019/9/30 0030.
  *
  *
  * Window Operations可以设置窗口的大小和滑动窗口的间隔来动态的获取当前Steaming的允许状态
  *
  *
  */
object windowsStreaming {

  def main(args: Array[String]): Unit = {
	//1.创建SparkConf并初始化SSC
	val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")
	val ssc = new StreamingContext(sparkConf, Seconds(3))


	//保存

	//4.通过KafkaUtil创建kafkaDSteam
	val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
	  ssc, "hadoop-01:2181", "atguigu", Map("atguigu" -> 3)
	)
	//窗口大小是采集周期的整数倍 5 窗口滑动步长也是采集周期的整数倍
	val windowsdstream: DStream[(String, String)] = kafkaDstream.window(Seconds(9),Seconds(3))

	val wordDStream: DStream[String] = windowsdstream.flatMap(t=>t._2.split(" "))

	val mapDstream: DStream[(String, Int)] = wordDStream.map((_,1))

	val result: DStream[(String, Int)] = mapDstream.reduceByKey(_+_)



	result.print()

	//6.启动SparkStreaming
	ssc.start()
	ssc.awaitTermination()
  }
}