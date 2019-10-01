package RDD

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

/**
  * Created by zld on 2019/10/1 0001.
  */
object Mysql_RDD {

  def main(args: Array[String]): Unit = {

	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Json_RDD")

	val sc = new SparkContext(config)

	//3.定义连接mysql的参数
	val driver = "com.mysql.jdbc.Driver"
	val url = "jdbc:mysql://localhost:3306/ssm"
	val userName = "root"
	val passWd = "smallming"

	//创建JdbcRDD
	/*val rdd = new JdbcRDD(sc, () => {
	  //获取数数据库连接对象
	  Class.forName(driver)
	  DriverManager.getConnection(url, userName, passWd)
	},
	  "select * from `user` where `id`>=? and id<=?;",
	  1,
	  3,
	  2,
	  r => (r.getInt(1), r.getString(2) ,r.getInt(3))
	)
	//打印最后结果
	println(rdd.count())
	rdd.foreach(println)
	sc.stop()*/

	//保存数据
	val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("zld",20),("zyj",20),("lll",30)))
	//driver端执行
	Class.forName(driver)
	val connection: Connection = DriverManager.getConnection(url,userName,passWd)
	/*
	dataRDD.foreach{			//executor端执行  有问题没法使用driver的变量  使用下面的方法
	  case (username,age)=>{
		val sql = "insert into user(username,password)values(?,?)"
		val statement: PreparedStatement = connection.prepareStatement(sql)
		statement.setString(1,username)
//		statement.setInt(2,password)
		statement.executeUpdate()


		statement.close()
		connection.close()
	  }
	}*/
	//对每一个分区操作  以下操作在每一个分区执行一遍
	dataRDD.foreachPartition(datas=>{

	  //executor端
	  Class.forName(driver)
	  val connection: Connection = DriverManager.getConnection(url,userName,passWd)

	  datas.foreach {
		case (username, age) => {
		  val sql = "insert into user(username,password)values(?,?)"
		  val statement: PreparedStatement = connection.prepareStatement(sql)
		  statement.setString(1, username)
		  //		statement.setInt(2,password)
		  statement.executeUpdate()
		  statement.close()
		}
	  }

	  connection.close()
	})


  }

}
