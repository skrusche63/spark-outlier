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

case class CommerceItem(id:String,price:Float)

case class CommerceTransaction(site:String,user:String,order:String,timestamp:Long,items:List[CommerceItem])

case class StateSequence(site:String,user:String,states:List[String])

case class LabeledPoint(
  label:String,features:Array[Double]
)

case class OutlierParameters(
  /*
   * The algorithm is applicable for outlier prediction and determines
   * which metric has to be used: the following metric algorithms are
   * supported:
   * 
   * a) missprob : Miss Probability
   * b) missrate : Miss Rate
   * c) entreduc : Entropy Reduction
   * 
   */
  algorithm:Option[String],
  /*
   * The parameter 'k' is restricted to outlier detection and determines
   * the number of outliers to be returned by the detector
   */
  k:Option[Int],
  /*
   * The parameter 'k' is restructed to outlier detection and specifies
   * the strategy to be used to determine cluster homogenity
   */
  strategy:Option[String],
  /*
   * The parameter 'threshold' is restricted to outlier prediction and
   * specifies the threshold for the prediction algorithm; the parameter
   * takes values between 0..1. The higher the value, the more likely it
   * is to have an outlier predicted
   */
  threshold:Option[Double]
)

case class OutlierRequest(
  /* 
   * Unique identifier to distinguish requests from each other;
   * the request is responsible for setting appropriate identifiers
   */
  uid:String,
  task:String,
  /*
   * The outlier computation method; actually two different approaches
   * are available: 'detect' means to find outliers by clustering, and
   * 'predict' means to find outliers from transaction sequences with
   * markov models
   */
  method:Option[String],
  parameters:Option[OutlierParameters],
  source:Option[OutlierSource]
)

case class OutlierResponse(
  uid:String,
  message:Option[String],
  detected:Option[List[(Double,LabeledPoint)]],
  predicted:Option[List[(String,String,Double,String)]],
  status:String
)

case class OutlierSource(
  /*
   * The path to a file on the HDFS or local file system
   * that holds a textual description of a sequence database
   */
  path:Option[String]
)

case class ElasticRequest()

case class FileRequest(
  path:String
)

object OutlierModel {
    
  implicit val formats = Serialization.formats(NoTypeHints)

  def serializeResponse(response:OutlierResponse):String = write(response)
  
  def deserializeRequest(request:String):OutlierRequest = read[OutlierRequest](request)
  
}

object OutlierMessages {
 
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)

  def NO_METHOD_PROVIDED(uid:String):String = String.format("""No method provided for uid '%s'.""", uid)

  def METHOD_NOT_SUPPORTED(uid:String):String = String.format("""The provided is not supported for uid '%s'.""", uid)

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
  val TRAINED:String = "trained"
    
  val STARTED:String = "started"
  val STOPPED:String = "stopped"
    
  val FINISHED:String = "finished"
  val RUNNING:String  = "running"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}