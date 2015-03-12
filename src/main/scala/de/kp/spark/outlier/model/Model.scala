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

import de.kp.spark.core.model._
 
object Algorithms {
  
  val KMEANS:String = "KMEANS"
  val MARKOV:String = "MARKOV"

  private def algorithms = List(KMEANS,MARKOV)  
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
    
}

object Serializer extends BaseSerializer

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