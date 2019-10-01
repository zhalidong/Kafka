package RDD

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.parsing.json.JSON
/**
  * Created by zld on 2019/10/1 0001.
  */
object Json_RDD {

  def main(args: Array[String]): Unit = {

	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Json_RDD")

	val sc = new SparkContext(config)

	val json = sc.textFile("in/user.json")

	val result  = json.map(JSON.parseFull)

	result.foreach(println)
	sc.stop()
  }

}
