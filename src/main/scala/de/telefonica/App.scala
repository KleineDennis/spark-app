package de.telefonica


import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */

case class Contract(rowkey: String, cti: String)
case class Profile(rowkey: String, cli: String)


object App {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    /////////////////////////////////////////////////////////////////////////////////////
    //  1. Direct access with Get Class
    /////////////////////////////////////////////////////////////////////////////////////
    {
      //Mobile Subscriber ISDN Number
      //VAZ + LKZ + ONK + SN + DDI
      //MSISDN: 00 49 8561 45678 12
      case class Msg(msisdn: String)
      val msg = Msg("004985614567812")

      val connection = ConnectionFactory.createConnection()
      val lkpMsisdnTable = connection.getTable(TableName.valueOf("digitalx:lkp_msisdn"))
      val profilerTable = connection.getTable(TableName.valueOf("digitalx:profiler2"))

      val getContractId = new Get(Bytes.toBytes(msg.msisdn)) //@param row rowkey
      getContractId.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cti"))
      val resultContractIdLookup = lkpMsisdnTable.get(getContractId)
      val contractId = resultContractIdLookup.getValue(Bytes.toBytes("d"), Bytes.toBytes("cti"))

      val getProfiler = new Get(contractId)
      getProfiler.addColumn(Bytes.toBytes("d"), Bytes.toBytes("cli")) // client_id
      val resultProfiler = profilerTable.get(getProfiler)

      //Additional filter criteria provided by CI and Build JSON
      val result = Bytes.toString(resultProfiler.getValue(Bytes.toBytes("d"), Bytes.toBytes("cli")))

      println(result)

      connection.close()
    }
    /////////////////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////////////////
    //  1b. Direct access with Get Class und get(List<Get>)   BulkLoad
    /////////////////////////////////////////////////////////////////////////////////////

    val getContractId1 = new Get(Bytes.toBytes("004985614567811")) //@param row rowkey
    val getContractId2 = new Get(Bytes.toBytes("004985614567813")) //@param row rowkey
    val getContractId3 = new Get(Bytes.toBytes("004985614567814")) //@param row rowkey

    val connection = ConnectionFactory.createConnection()

    val lkpMsisdnTable = connection.getTable(TableName.valueOf("digitalx:lkp_msisdn"))
    val list: util.List[Get] = new util.ArrayList[Get]()
    list.add(getContractId1)
    list.add(getContractId2)
    list.add(getContractId3)
    val resultContractIdLookup = lkpMsisdnTable.get(list)

    resultContractIdLookup.foreach(println)

    /////////////////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////////////////
    //  2. RDD with filter and join
    /////////////////////////////////////////////////////////////////////////////////////
//    {
//      val spark = SparkSession
//        .builder
//        .appName("highusage")
//        .master("local[*]")
//        .getOrCreate()
//
//      val conf = HBaseConfiguration.create()
//      conf.set(TableInputFormat.INPUT_TABLE, "digitalx:lkp_msisdn")
//      conf.set(TableInputFormat.SCAN, "d")
//      conf.set(TableInputFormat.SCAN_COLUMNS, "cti") //"cti ... ..."
//
//      val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,
//        classOf[TableInputFormat],
//        classOf[ImmutableBytesWritable],
//        classOf[Result])
//
//        //mit Lookup
//        val valuesRDD = hBaseRDD.lookup(Bytes.toBytes("004985614567812"))
////      val filteredRDD = hBaseRDD.filter(key => Bytes.toString(key._1.get).equals("004985614567812"))
////      val valueRDD = filteredRDD.map(result => Bytes.toString(result._2.value))
////
////      valueRDD.foreach(println)
//
//
//      val conf2 = HBaseConfiguration.create()
//      conf2.set(TableInputFormat.INPUT_TABLE, "digitalx:profiler2")
//      conf2.set(TableInputFormat.SCAN, "d")
//      conf2.set(TableInputFormat.SCAN_COLUMNS, "cli") //"cli ... ..."
//
//
//      val hBaseRDD2 = spark.sparkContext.newAPIHadoopRDD(conf2,
//        classOf[TableInputFormat],
//        classOf[ImmutableBytesWritable],
//        classOf[Result])
//
////      val filteredRDD2 = hBaseRDD2.filter(key => Bytes.toString(key._1.get).equals("1234567"))
////      val valueRDD2 = filteredRDD2.map(result => Bytes.toString(result._2.value))
////
////      valueRDD2.foreach(println)
//
////      val contracts = hBaseRDD.filter(key => Bytes.toString(key._1.get).equals("004985614567812")).map(a => (Bytes.toString(a._2.value), Bytes.toString(a._1.get)))
//      val contractsRDD = hBaseRDD.filter(key => List("004985614567812", "004985614567814").contains(Bytes.toString(key._1.get))).map(a => (Bytes.toString(a._2.value), Bytes.toString(a._1.get)))
//      contractsRDD.foreach(println)
//
//      val profilesRDD = hBaseRDD2.map(a => (Bytes.toString(a._1.get),Bytes.toString(a._2.value)))
//      profilesRDD.foreach(println)
//
//      val clientsRDD = contractsRDD.join(profilesRDD)
//      clientsRDD.foreach(println)
//    }
      /////////////////////////////////////////////////////////////////////////////////////


    /////////////////////////////////////////////////////////////////////////////////////
    //  3. Dataframes with filter
    /////////////////////////////////////////////////////////////////////////////////////
//    {
//
//      val spark = SparkSession
//        .builder
//        .appName("highusage")
//        .master("local[*]")
//        .getOrCreate()
//
//      import spark.implicits._
//
//      val conf = HBaseConfiguration.create()
//      conf.set(TableInputFormat.INPUT_TABLE, "digitalx:lkp_msisdn")
//      conf.set(TableInputFormat.SCAN, "d")
//      conf.set(TableInputFormat.SCAN_COLUMNS, "cti") //"cti ... ..."
//
//      val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,
//        classOf[TableInputFormat],
//        classOf[ImmutableBytesWritable],
//        classOf[Result])
//
//      val resultRDD = hBaseRDD.values
//
//      def parse(result: Result): Contract = {
//        val rowkey = Bytes.toString(result.getRow())
//        val p0 = rowkey.split(" ")(0)
//        val p1 = Bytes.toString(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("cti")))
//        Contract(p0, p1)
//      }
//
//      val contractRDD = resultRDD.map(parse)
//      val dfContract = contractRDD.toDF()
//
//      dfContract.show()
//
//
//      val conf2 = HBaseConfiguration.create()
//      conf2.set(TableInputFormat.INPUT_TABLE, "digitalx:profiler2")
//      conf2.set(TableInputFormat.SCAN, "d")
//      conf2.set(TableInputFormat.SCAN_COLUMNS, "cli") //"cli ... ..."
//
//      val hBaseRDD2 = spark.sparkContext.newAPIHadoopRDD(conf2,
//        classOf[TableInputFormat],
//        classOf[ImmutableBytesWritable],
//        classOf[Result])
//
//      val resultRDD2 = hBaseRDD2.values
//
//      def parse2(result: Result): Profile = {
//        val rowkey = Bytes.toString(result.getRow())
//        val p0 = rowkey.split(" ")(0)
//        val p1 = Bytes.toString(result.getValue(Bytes.toBytes("d"), Bytes.toBytes("cli")))
//        Profile(p0, p1)
//      }
//
//      val profilerRDD = resultRDD2.map(parse2)
//      val dfProfiler = profilerRDD.toDF()
//
//      dfProfiler.show()
//
//
//      val clientsRDD = dfContract.filter("rowkey = 004985614567812")
//        .join(dfProfiler, dfContract.col("cti").equalTo(dfProfiler.col("rowkey")))
//
//      clientsRDD.show()
//
//      dfContract.createOrReplaceTempView("lkp_msisdn")
//      dfProfiler.createOrReplaceTempView("profiler2")
//
//      val sqlDF = spark.sql("""SELECT *
//                               FROM  lkp_msisdn JOIN  profiler2
//                               ON lkp_msisdn.cti == profiler2.rowkey
//                               WHERE lkp_msisdn.rowkey = '004985614567812' """)
//
//      sqlDF.show()
//
//    }
  }

}

