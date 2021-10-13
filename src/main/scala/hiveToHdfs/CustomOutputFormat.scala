package hiveToHdfs

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.mapred.{FileOutputFormat, JobConf}

class CustomOutputFormat  extends MultipleTextOutputFormat[Any, Any]{
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    //重写generateFileNameForKeyValue方法，该方法是负责自定义生成文件的文件名
    val fileName = key.asInstanceOf[String] + ".txt"
    fileName
  }

  //因为saveAsHadoopFile是以key,value的形式保存文件，写入文件之后的内容也是，按照key value的形式写入，k,v之间用空格隔开，这里我只需要写入value的值，不需要将key的值写入到文件中个，所以我需要重写<br>   *该方法，让输入到文件中的key为空即可，当然也可以进行领过的变通，也可以重写generateActuralValue(key:Any,value:Any),根据自己的需求来实现<br>   */
  override def generateActualKey(key: Any, value: Any): Any = {
    //对生成的value进行转换为字符串，当然源码中默认也是直接返回value值，如果对value没有特殊处理的话，不需要重写该方法
    null
  }

  override def checkOutputSpecs(ignored: FileSystem, job: JobConf): Unit = {
    var outDir: Path = FileOutputFormat.getOutputPath(job)
    if (outDir != null) {
      //val fs: FileSystem = ignored
      //outDir = fs.makeQualified(outDir)
      FileOutputFormat.setOutputPath(job, outDir)
    }
  }
}
