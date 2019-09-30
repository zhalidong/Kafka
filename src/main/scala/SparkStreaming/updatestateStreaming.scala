package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by zld on 2019/9/30 0030.
  *
  *
  * UpdateStateByKey有状态算子 需要checkpoint
  *
  *
  */
object updatestateStreaming {

  def main(args: Array[String]): Unit = {
	//1.创建SparkConf并初始化SSC
	val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")
	val ssc = new StreamingContext(sparkConf, Seconds(5))


	//保存数据的状态,需要设定检查点路径
	ssc.sparkContext.setCheckpointDir("cp")

	//4.通过KafkaUtil创建kafkaDSteam
	val kafkaDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
	  ssc, "hadoop-01:2181", "atguigu", Map("atguigu" -> 3)
	)
	val wordDStream: DStream[String] = kafkaDstream.flatMap(t=>t._2.split(" "))

	val mapDstream: DStream[(String, Int)] = wordDStream.map((_,1))

	val wordtosumdstream: DStream[(String, Int)] = mapDstream.updateStateByKey{
	  case ( seq,buffer)=>{
		val sum = buffer.getOrElse(0)+seq.sum
		Option(sum)
	  }
	}

	wordtosumdstream.print()

	//6.启动SparkStreaming
	ssc.start()
	ssc.awaitTermination()
  }
}