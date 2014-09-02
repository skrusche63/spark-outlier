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

case class LabeledPoint(
  label:String,features:Array[Double]
)

case class OutlierParameters(
  /*
   * Number of outliers to be returned by the algorithm
   */
  k:Int,
  /*
   * Strategy to determine cluster homogenity
   */
  strategy:Option[String]    
)

case class OutlierRequest(
  /* 
   * Unique identifier to distinguish requests from each other;
   * the request is responsible for setting appropriate identifiers
   */
  uid:String,
  task:String,
  parameters:Option[OutlierParameters],
  source:Option[OutlierSource]
)

case class OutlierResponse(
  uid:String,
  message:Option[String],
  outliers:Option[List[(Double,LabeledPoint)]],
  status:String
)

case class OutlierSource(
  /*
   * The path to a file on the HDFS or local file system
   * that holds a textual description of a sequence database
   */
  path:Option[String],
  nodes:Option[String],
  port:Option[String],
  resource:Option[String],
  query:Option[String],
  fields:Option[String]
)

case class ElasticRequest(
  nodes:String,
  port:String,
  resource:String,
  query:String
)

case class FileRequest(
  path:String
)

object OutlierModel {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializeResponse(response:OutlierResponse):String = write(response)
  
  def deserializeRequest(request:String):OutlierRequest = read[OutlierRequest](request)
  
}

object OutlierMessages {
 
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameter k or strategy is missing for uid '%s'.""", uid)

  def NO_PARAMETERS_PROVIDED(uid:String):String = String.format("""No parameters provided for uid '%s'.""", uid)

  def NO_SOURCE_PROVIDED(uid:String):String = String.format("""No source provided for uid '%s'.""", uid)

  def OUTLIER_DETECTION_STARTED(uid:String) = String.format("""Outlier detection started for uid '%s'.""", uid)

  def OUTLIERS_DO_NOT_EXIST(uid:String):String = String.format("""The outliers for uid '%s' do not exist.""", uid)

  def TASK_ALREADY_STARTED(uid:String):String = String.format("""The task with uid '%s' is already started.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = String.format("""The task with uid '%s' does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = String.format("""The task '%s' is unknown for uid '%s'.""", task, uid)
   
}

object OutlierStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val STOPPED:String = "stopped"
    
  val FINISHED:String = "finished"
  val RUNNING:String  = "running"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}