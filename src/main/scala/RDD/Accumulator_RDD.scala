package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zld on 2019/10/1 0001.
  *
  * 自定义累加器
  *
  *
  */
object Accumulator_RDD {

  def main(args: Array[String]): Unit = {

	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Json_RDD")

	val sc = new SparkContext(config)

	val dataRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

	/*val i: Int = dataRDD.reduce(_+_)

	println(i)*/
	/**
	  * TODO sum这个变量在driver端,然后序列化到executor端进行累加每个分区各自累加的结果是3 7 但是每个分区后的数据没有进行累加 3+7
	  * executor端的数据也没有返回给driver端
	  *
	  *
	  */
	/*var sum=0			//driver
	dataRDD.foreach(i=>sum=sum+i)	//i=>sum=sum+i 是在executor  driver端sum这个变量序列化到executor端
	println(sum)//driver端打印*/


	//使用累加器来共享变量 来累加数据 并返回给driver端

	//创建累加器对象
	val accumulator: LongAccumulator = sc.longAccumulator
	dataRDD.foreach{
	  case  i =>{
			//执行累加器功能
			accumulator.add(i)
	  }
	}
	//获取累加器的值
	println("sum = "+accumulator.value)

	sc.stop()
  }

}

//声明累加器
