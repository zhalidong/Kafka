package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zld on 2019/9/30 0030.
  */
object WordCount {

  def main(args: Array[String]): Unit = {

	//1.初始化Spark配置信息
	val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

	//2.初始化SparkStreamingContext
	val ssc = new StreamingContext(sparkConf, Seconds(5))

	//3.通过监控端口创建DStream，读进来的数据为一行行
	val lineStreams = ssc.socketTextStream("hadoop-01", 9999)

	val wordDstream: DStream[String] = lineStreams.flatMap(x=>x.split(" "))

	val mapDstream: DStream[(String, Int)] = wordDstream.map((_,1))

	val wordtosumDstream: DStream[(String, Int)] = mapDstream.reduceByKey(_+_)

	wordtosumDstream.print()

	ssc.start()
	ssc.awaitTermination()


  }


}
