package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zld on 2019/9/20 0020.
  *
  *所有算子的计算功能都是executor来做的
  *
  */
object Spark05_Driver_Executor {

  def main(args: Array[String]): Unit = {
	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark02_RDD")

	//创建spark上下文对象
	val sc = new SparkContext(config)

	val listRDD: RDD[Int] = sc.makeRDD(1 to 4)

	val i=10

		//(_*2)在executor执行  其他都是在driver执行  i为了传输需要序列化
	val mapRDD: RDD[Int] = listRDD.map(_*i)
	mapRDD.collect().foreach(println)

	sc.stop()

  }

}
