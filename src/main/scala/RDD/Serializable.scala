package RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zld on 2019/9/30 0030.
  *
  *在实际开发中我们往往需要自己定义一些对于RDD的操作，
  * 那么此时需要主要的是，初始化工作是在Driver端进行的，
  * 而实际运行程序是在Executor端进行的，这就涉及到了跨进程通信，是需要序列化的
  *
  * Search没有进行序列化
  *
  */
object Serializable {

  def main(args: Array[String]): Unit = {

	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serializable")

	val sc = new SparkContext(config)


	//2.创建一个RDD
	val rdd: RDD[String] = sc.parallelize(Array("hadoop", "spark", "hive", "atguigu"))

	//3.创建一个Search对象  dirver端 对象不序列化 没法发送到executor
	val search = new Search("h")

	//4.运用第一个过滤函数并打印结果
	val match1: RDD[String] = search.getMatche1(rdd)
	match1.collect().foreach(println)



	sc.stop()
  }
}

class Search(query:String) extends Serializable {

  //过滤出包含字符串的数据
  def isMatch(s: String): Boolean = {
	s.contains(query)
  }

  //过滤出包含字符串的RDD
  def getMatche1 (rdd: RDD[String]): RDD[String] = {
	rdd.filter(isMatch)	//executor端
  }

  //过滤出包含字符串的RDD
  def getMatche2(rdd: RDD[String]): RDD[String] = {
	rdd.filter(x => x.contains(query))
  }

}

