package com.feichen

import java.util
import scala.collection.JavaConversions._
import org.yaml.snakeyaml.Yaml

class Utils {
  def parseConfig(filePath: String): util.Map[String, Object] = {
    val stream = getClass.getClassLoader.getResourceAsStream(filePath)
    val yaml = new Yaml()
    val obj = yaml.load(stream).asInstanceOf[java.util.Map[String, Object]]
    obj
  }


  def transArrayToString(parm: java.util.Map[String, Object], arrName: String) = {
    val arr = parm(arrName)
      .asInstanceOf[java.util.ArrayList[String]]
      .toArray
    var str = arr(0)
    for(i <- 1 until arr.length){
      str = str + "," + arr(i)
    }
    str.toString
  }

}
