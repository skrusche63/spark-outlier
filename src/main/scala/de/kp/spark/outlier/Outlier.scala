package de.kp.spark.outlier

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

trait RuleJSON {}

case class Feature(field:String,value:Double)

case class Detection(
  distance:Double,label:String,features:List[Feature]) extends RuleJSON {
  
  implicit val formats = Serialization.formats(ShortTypeHints(List()))
  
  def toJSON:String = write(this)
  
}

case class Prediction(
  site:String,user:String,states:List[String],metric:Double,flag:String) extends RuleJSON {
  
  implicit val formats = Serialization.formats(ShortTypeHints(List()))
  
  def toJSON:String = write(this)
  
}