package de.kp.spark.outlier.model
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Outlier project
* (https://github.com/skrusche63/spark-outlier).
* 
* Spark-Outlier is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Outlier is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Outlier. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.core.model._

case class FField(field:String,value:Double)

case class FDetection(
  distance:Double,label:String,features:List[FField])

case class FDetections(items:List[FDetection])
  
case class BDetection(
  site:String,user:String,states:List[String],metric:Double,flag:String)

case class BDetections(items:List[BDetection])
/**
 * A LabeledPoint describes a combination of a feature
 * vector and an assigned label. Each data record is
 * also uniquely identifier by the 'id' parameter.
 * 
 * This parameter is usually equal to the row descriptor
 * of the data record (see vector description).
 * 
 */
case class LabeledPoint(
  id:Long,label:String,features:Array[Double]
)

case class BOutliers(items:List[(String,String,List[String],Double,String)])

case class FOutliers(items:List[(Double,LabeledPoint)])

object Algorithms {
  
  val KMEANS:String = "KMEANS"
  val MARKOV:String = "MARKOV"

  private def algorithms = List(KMEANS,MARKOV)  
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
    
}

object Sources {
  /* The names of the data source actually supported */
  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PARQUET:String = "PARQUET"   
    
  private val sources = List(FILE,ELASTIC,JDBC,PARQUET)
  def isSource(source:String):Boolean = sources.contains(source)
    
}

object Serializer extends BaseSerializer {

  /*
   * Support for serialization and deserialization of detections
   */
  def serializeBDetections(detections:BDetections):String = write(detections)
  def serializeFDetections(detections:FDetections):String = write(detections)

  def deserializeBDetections(detections:String):BDetections = read[BDetections](detections)
  def deserializeFDetections(detections:String):FDetections = read[FDetections](detections)
  /*
   * Support for serialization and deserialization of outliers
   */  
  def serializeBOutliers(outliers:BOutliers):String = write(outliers)
  def serializeFOutliers(outliers:FOutliers):String = write(outliers)
  
  def deserializeBOutliers(outliers:String):BOutliers = read[BOutliers](outliers)
  def deserializeFOutliers(outliers:String):FOutliers = read[FOutliers](outliers)
  
}

object Messages extends BaseMessages {
 
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)

  def NO_METHOD_PROVIDED(uid:String):String = String.format("""No method provided for uid '%s'.""", uid)

  def METHOD_NOT_SUPPORTED(uid:String):String = String.format("""The provided is not supported for uid '%s'.""", uid)

  def OUTLIER_DETECTION_STARTED(uid:String) = String.format("""Outlier detection started for uid '%s'.""", uid)

  def OUTLIERS_DO_NOT_EXIST(uid:String):String = String.format("""The outliers for uid '%s' do not exist.""", uid)
  
}

object OutlierStatus extends BaseStatus {
  
  val DATASET:String = "dataset" 
  val TRAINED:String = "trained"
    
  val STARTED:String = "started"
  val STOPPED:String = "stopped"
    
  val FINISHED:String = "finished"
  val RUNNING:String  = "running"
    
}