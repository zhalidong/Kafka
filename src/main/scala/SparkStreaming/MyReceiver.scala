package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zld on 2019/9/30 0030.
  *
  * 自定义采集器
  *
  */
object MyReceiver {

  def main(args: Array[String]): Unit = {

	//1.初始化Spark配置信息
	val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

	//2.初始化SparkStreamingContext
	val ssc = new StreamingContext(sparkConf, Seconds(5))

	//3.指定文件夹
	val fileStreams = ssc.textFileStream("test")

	val wordDstream: DStream[String] = fileStreams.flatMap(x=>x.split(" "))

	val mapDstream: DStream[(String, Int)] = wordDstream.map((_,1))

	val wordtosumDstream: DStream[(String, Int)] = mapDstream.reduceByKey(_+_)

	wordtosumDstream.print()

	ssc.start()
	ssc.awaitTermination()


  }


}

//声明采集器
//1.继承Receiver
//2.重写方法 onstart, onstop
class  MyReceiver(host:String,port:Int) extends Receiver(StorageLevel.MEMORY_ONLY){
  var socket :java.net.Socket= null

  def receive()={
	socket=new java.net.Socket(host,port)



  }

  override def onStart(): Unit = {
	new Thread(new Runnable {
	  override def run(): Unit = {
		receive()
	  }
	}).start()
  }

  override def onStop(): Unit = {

  }
}
