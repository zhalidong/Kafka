package RDD

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zld on 2019/10/1 0001.
  */
object Hbase_RDD {

  def main(args: Array[String]): Unit = {

	val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Hbase_RDD")

	val sc = new SparkContext(config)

	//构建HBase配置信息
	val conf: Configuration = HBaseConfiguration.create()
	conf.set(TableInputFormat.INPUT_TABLE, "rddtable")

	val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
	  classOf[ImmutableBytesWritable],
	  classOf[Result])
	hbaseRDD.foreach{
	  case (rowkey,result)=>{

	  }
	}
  }

}
