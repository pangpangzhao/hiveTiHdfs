package hiveToHdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object HiveToHdfs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .enableHiveSupport()
      .config("spark.rdd.compress", "false")
      .config("hive.exec.compress.output", "false")
      .config("spark.hadoop.mapreduce.output.fileoutputformat.compress", "false")
      .appName(this.getClass.getSimpleName.dropRight(1))
      .getOrCreate()
    val sc = spark.sparkContext
    // 关闭 success文件的输出
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
//    spark.conf.set("spark.rdd.compress", "false")
//    spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", "false")
//    spark.conf.set("hive.exec.compress.output", "false")
//    spark.conf.set("mapreduce.output.fileoutputformat.compress", "false")

    val sdf = new SimpleDateFormat();// 格式化时间
    sdf.applyPattern("yyyy-MM-dd");// a为am/pm的标记
    var date = new Date();// 获取当前时间
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_MONTH,-1)
    date = calendar.getTime
    val dateMy = sdf.format(date)

    var dealDate = dateMy
    var sql =
      s"""
        |SELECT
        |uv
        |from hdp_lbg_ecdata_dw_defaultdb.d_app_useranalysis
        |where stat_date='${dealDate}'
        |and product_name='all'
        |and channel_type='all'
        |and channel_media='all'
        |and business_type='all'
        |and version='all'
        |and pinpai='all'
        |and devices_type='all'
        |AND operater_system='all'
        |and province='all'
        |and city='all'
        |""".stripMargin
    var hdfs_path = "/home/hdp_lbg_ecdata_dw/resultdata/sketch/ai_projects/PredictDau_weekly"

    if(args.length==3) {
      sql = args(1)
      dealDate = args(0)
      hdfs_path = args(2)
    }

    sql = sql.replaceAll("dealDate",dealDate)
    println(sql)

    import spark.implicits._
    val rdd = spark.sql(sql).rdd.map(row=>{
      val str = row.toString().replaceAll("\\[", "").replaceAll("\\]", "")
      ("yesterday_dau",str)
    })

//   rdd.take(1).foreach(println(_))

    val fileSystem = FileSystem.get(new URI(hdfs_path), new Configuration())
    val path = new Path(hdfs_path)
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }
    rdd.saveAsHadoopFile(hdfs_path,classOf[String],classOf[String],classOf[CustomOutputFormat])
    sc.stop()
    spark.close()

  }
}
