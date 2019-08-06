package com.feichen

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * create by feichen   2019-08-06
  */
class KuduHandleExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveToKudu")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.orc.filterPushdown", "true")
      .config("spark.sql.warehouse.dir", "s3://test/spark-warehouse")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    if(args.length != 2){
      println("Wrong args! Check it!")
    }
    //val kuduTableName = args(0)
    val dateTime = args(1)

    //TODO: 实例化KuduHandle
    val kuduHandle = new KuduHandle(spark)

    //TODO: 设置kudu表名
    val kuduTableName = "kudu_handle_test"

    //TODO: 通过Utils工具类获得列名
    val util = new Utils
    val parm = util.parseConfig("createKuduTable.yml")
    val rangeCol = util.transArrayToString(parm,"rangeKeyColumn")
    val hashCols = util.transArrayToString(parm,"hashKeyColumns")
    val simpleCols = util.transArrayToString(parm,"simpleBaseColumns")

    try {
      //TODO: 调用KuduHandle.createTable()创建表
      kuduHandle.createTable(kuduTableName, rangeCol, hashCols, simpleCols)

      //TODO: 生成DataFrame格式数据，调用KuduHandle.insertTable()插入数据
      val sql =
        s"""
           |select '${dateTime}' as ${rangeCol},
           |${hashCols},
           |${simpleCols}
           |from feichen_test.kudu_hive where dt='${dateTime}'
        """.stripMargin
      val algoDF = spark.sql(sql)
      kuduHandle.insertTable_addRangePartition(algoDF, kuduTableName,rangeCol, dateTime)

      //TODO: 调用KuduHandle.addColumns()添加列
      val addColumns = "add1,add2,add3,add4,add5"
      kuduHandle.addColumns(kuduTableName, addColumns)

      //TODO: 调用KuduHandle.upsertTable()更新插入数据 （除小时级数据入库外，建议都使用upsertTable()插入数据）
      val sql1 =
        s"""
           |select '${dateTime}' as ${rangeCol},
           |${hashCols},
           |simple_key1 as add1,
           |simple_key2 as add2,
           |simple_key3 as add3,
           |simple_key4 as add4,
           |simple_key5 as sdd5
           |from feichen_test.kudu_hive where dt='${dateTime}' limit 100
      """.stripMargin
      val df1 = spark.sql(sql1)
      kuduHandle.upsertTable(df1, kuduTableName)

      //TODO: 调用KuduHandle.readTable()读取数据  (可通过sparkSQL查询kudu数据，kudu引擎自动解析优化)
      kuduHandle.readTable(kuduTableName).createOrReplaceTempView("kudu")
      //val kuduDF = kuduHandle.readTable(spark, kuduTableName)
      //kuduDF.createOrReplaceTempView("kudu")
      /**（以下仅针对range分区字段——hour_id ，其他字段可随意使用）
        *
        *注意：kudu引擎 *不支持* sparkSQL的 >、<、or 这些谓词下推，此时会变成全表扫描，效率极低！！！
        *      要使用  *between*  来限制查询范围，
        *      对于非连续的跳跃查询，可以使用 *in* 来限制查询范围。
        *
        * 切记：除非你想读取全表数据，否则必须对“hour_id”字段使用between或者in来限制查询范围！！！
        */
      val df = spark.sql(s"select ${rangeCol},${hashCols},add1,add2 from kudu where range_key between '2019060101' and '2019060205'")
      val output = "s3://test/test_mydata8"
      FileSystem.get(new URI(s"s3://test"), spark.sparkContext.hadoopConfiguration).delete(new Path(output), true)
      df.write.mode(SaveMode.Overwrite).csv(output)

      //TODO: 调用KuduHandle.deleteColumns()删除列
      kuduHandle.deleteColumns(kuduTableName,"add2,add3,add5")

      //TODO: 调用KuduHandle.dropRangePartition()删除分区   (删除分区，会同时删除分区内的数据。慎用！！！)
      kuduHandle.dropRangePartition(kuduTableName,rangeCol,"2019072207","2019072208")

      //TODO: 调用KuduHandle.dropTable()删除表
      kuduHandle.dropTable(kuduTableName)
    }finally {
      spark.stop()
    }
  }
}