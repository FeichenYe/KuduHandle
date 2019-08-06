package com.feichen

import java.util

import org.apache.kudu.ColumnSchema.{CompressionAlgorithm, Encoding}
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client.{AlterTableOptions, CreateTableOptions}
import org.apache.kudu.spark.kudu.{KuduContext, KuduWriteOptions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import collection.JavaConverters._

/**
  * create by feichen   2019-08-06
  */
class KuduHandle(spark: SparkSession) {

  private val utils = new Utils
  private val kuduConf = utils.parseConfig("kuduConfig.yml")
  private val kuduMaster = utils.transArrayToString(kuduConf,"kuduMaster")

  private val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)
  private val kuduClient = kuduContext.syncClient


  /**
    *新建kudu表。这里range分区字段强制为一个字段
    * @param kuduTableName      新建kudu表名
    * @param rangeKeyColumn     range分区字段
    * @param hashKeyColumns     hash分区字段，由多个主键组成，逗号分隔
    * @param simpleBaseColumns  kudu表非主键字段，逗号分隔
    */
  def createTable(kuduTableName: String,
                  rangeKeyColumn: String,
                  hashKeyColumns: String,
                  simpleBaseColumns: String): Unit ={
    //TODO:定义列schema
    val kuduCols = new util.ArrayList[ColumnSchema]()

    val rangeCol = new ColumnSchema.ColumnSchemaBuilder(rangeKeyColumn, Type.STRING).key(true).nullable(false)
      .encoding(Encoding.DICT_ENCODING).compressionAlgorithm(CompressionAlgorithm.LZ4)
    kuduCols.add(rangeCol.build())

    for(colName <- hashKeyColumns.split(",")){
      val col = new ColumnSchema.ColumnSchemaBuilder(colName, Type.STRING).key(true).nullable(false).
        encoding(Encoding.PLAIN_ENCODING).compressionAlgorithm(CompressionAlgorithm.LZ4)
      kuduCols.add(col.build())
    }

    for(colName <- simpleBaseColumns.split(",")){
      val col = new ColumnSchema.ColumnSchemaBuilder(colName,Type.STRING).key(false).nullable(true)
        .encoding(Encoding.PLAIN_ENCODING).compressionAlgorithm(CompressionAlgorithm.LZ4)
      kuduCols.add(col.build())
    }
    val schema = new Schema(kuduCols)

    //TODO:定义分区schema
    val hashKeyArr = hashKeyColumns.split(",")
    var hashKeyList = List(hashKeyArr(0))
    for(i <- 1 until hashKeyArr.length){
      hashKeyList = hashKeyList ++ List(hashKeyArr(i))
    }
    val hashKey = hashKeyList.asJava

    val lower = schema.newPartialRow()
    lower.addString(rangeKeyColumn,"2018010100")
    val upper = schema.newPartialRow()
    upper.addString(rangeKeyColumn,"2019010100")

    val kuduTableOptions = new CreateTableOptions()
    kuduTableOptions
      .addHashPartitions(hashKey, 10)
      .setRangePartitionColumns(List(rangeKeyColumn).asJava)
      .addRangePartition(lower,upper)
      .setNumReplicas(3)

    //TODO:调用create Table api
    kuduContext.createTable(kuduTableName, schema, kuduTableOptions)
  }


  /**
    * 删除kudu表。慎用！！！！！！
    * @param kuduTableName  待删除kudu表名
    */
  def dropTable(kuduTableName: String): Unit ={
    kuduClient.deleteTable(kuduTableName)
  }


  /**
    * 向kudu表插入数据
    * @param df     包含待插入列数据的dataframe
    * @param kuduTableName  插入数据的目标kudu表
    */
  def insertTable(df: DataFrame, kuduTableName: String): Unit ={
    kuduContext.insertRows(df,kuduTableName,new KuduWriteOptions(ignoreNull = true,ignoreDuplicateRowErrors = true))
  }

  /**
    * 通过dateTime添加新的小时级range分区，向kudu表插入数据，此函数用于小时数据入库
    * @param df     包含待插入列数据的dataframe
    * @param kuduTableName  插入数据的目标kudu表
    * @param rangeKey       range分区字段
    * @param dateTime       小时级时间，用于添加新的range分区
    */
  def insertTable_addRangePartition(df: DataFrame,
                  kuduTableName: String,
                  rangeKey: String,
                  dateTime: String): Unit ={
    val upperBound = (dateTime.toInt+1).toString
    try{
      addRangePartition(kuduTableName,rangeKey,dateTime,upperBound)
    }catch {
      case e: Exception => {
        println("可能重复添加分区")
        println(e)
      }
    }
    //insert data
    kuduContext.insertRows(df,kuduTableName,new KuduWriteOptions(ignoreNull = true,ignoreDuplicateRowErrors = true))
  }


  /**
    * 更新插入数据
    * @param df     包含待插入列数据的dataframe
    * @param kuduTableName  目标kudu表
    */
  def upsertTable(df: DataFrame, kuduTableName: String): Unit ={
    kuduContext.upsertRows(df,kuduTableName,new KuduWriteOptions(ignoreNull = true))
  }


  /**
    * 给指定kudu表添加列，columnsString 字段可以包含多个列名，用逗号隔开 (此处添加的列皆为非主键列)
    * @param kuduTableName  目标kudu表
    * @param columnsString  待添加的列，可包含多个列名，用逗号隔开
    */
  def addColumns(kuduTableName: String, columnsString: String): Unit ={
    val ato = new AlterTableOptions()
    for(colName <- columnsString.split(",")){
      val col = new ColumnSchema.ColumnSchemaBuilder(colName,Type.STRING).key(false).nullable(true)
        .encoding(Encoding.PLAIN_ENCODING).compressionAlgorithm(CompressionAlgorithm.SNAPPY)
      ato.addColumn(col.build())
    }

    kuduClient.alterTable(kuduTableName, ato)
  }


  /**
    * 删除kudu列，慎用！！！！！！
    * @param kuduTableName  目标kudu表
    * @param columnsString  待删除的列，可包含多个列名，用逗号隔开
    */
  def deleteColumns(kuduTableName: String, columnsString: String): Unit ={
    val ato = new AlterTableOptions()
    for(colName <- columnsString.split(",")){
      ato.dropColumn(colName)
    }
    kuduClient.alterTable(kuduTableName, ato)
  }


  /**
    * 读取kudu表数据，返回一个dataframe
    * @param kuduTableName  目标kudu表
    * @return   包含目标数据的dataframe
    */
  def readTable(kuduTableName: String): DataFrame ={
    val df = spark.read
      .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> kuduTableName))
      .format("kudu").load
    df
  }


  /**
    * 添加range分区，分区的范围不能相互覆盖
    * @param kuduTableName  目标kudu表
    * @param rangeKey  range分区字段
    * @param lower  分区的下限
    * @param upper  分区的上限
    */
  def addRangePartition(kuduTableName: String, rangeKey: String, lower: String, upper: String): Unit ={
    val table = kuduClient.openTable(kuduTableName)
    val schema = table.getSchema
    val lowerBound= schema.newPartialRow()
    lowerBound.addString(rangeKey,lower)
    val upperBound = schema.newPartialRow()
    upperBound.addString(rangeKey,upper)
    val ato = new AlterTableOptions
    //add range partition
    ato.addRangePartition(lowerBound,upperBound)
    kuduClient.alterTable(kuduTableName,ato)
  }


  /**
    * 删除kudu表range分区，会同时删除分区内的数据。慎用！！！
    * @param kuduTableName  目标kudu表
    * @param rangeKey   range分区字段
    * @param lower      分区的下限
    * @param upper      分区的上限
    */
  def dropRangePartition(kuduTableName: String, rangeKey: String, lower: String, upper: String): Unit ={
    val table = kuduClient.openTable(kuduTableName)
    val schema = table.getSchema
    val lowerBound= schema.newPartialRow()
    lowerBound.addString(rangeKey,lower)
    val upperBound = schema.newPartialRow()
    upperBound.addString(rangeKey,upper)
    val ato = new AlterTableOptions
    //drop range partition
    ato.dropRangePartition(lowerBound,upperBound)
    kuduClient.alterTable(kuduTableName,ato)
  }


  /**
    * 删除行，df中必须包含所有的主键字段  (如果是批量删除，可以考虑删除分区)
    * @param df   包含所有主键的DataFrame
    * @param kuduTableName  目标kudu表
    */
  def deleteRows(df: DataFrame, kuduTableName: String): Unit ={
    kuduContext.deleteRows(df,kuduTableName)
  }
}