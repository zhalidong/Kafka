package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by zld on 2019/9/30 0030.
  *
  *
  * Transform
  *
  *
  */
object transformStreaming {

  def main(args: Array[String]): Unit = {
	//1.创建SparkConf并初始化SSC
	val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaSparkStreaming")
	val ssc = new StreamingContext(sparkConf, Seconds(3))

	val sockStreams = ssc.socketTextStream("hadoop-01", 9999)


	//TODO 代码(Driver)
	//val a = 1 只执行一遍
	sockStreams.map{
	  case x=>{
		//TODO 代码 (Executer)
		//val a = 1 每个executeor都执行
		x
	  }
	}

	//todo 代码(Driver)  代码只执行一遍
/*	sockStreams.transform{
	  case rdd=>{
		//todo 代码 Driver  代码根据周期性时间执行
		rdd.map{
		  case x=> {
			//todo 代码 Executor
			x
		  }
		}
	  }
	}*/

	//保存


	//即将函数 func 用于产生于 stream的每一个RDD 它用来对DStream中的RDD运行任意计算
	//sockStreams.foreachRDD()


	//6.启动SparkStreaming
	ssc.start()
	ssc.awaitTermination()
  }
}