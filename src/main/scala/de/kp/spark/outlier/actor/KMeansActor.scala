package de.kp.spark.outlier.actor
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.outlier.{KMeansDetector,RequestContext}
import de.kp.spark.outlier.model._

import de.kp.spark.core.source.VectorSource
import de.kp.spark.core.source.handler.VectorHandler

import de.kp.spark.core.redis.RedisDB

import de.kp.spark.outlier.spec.VectorSpec
import scala.collection.mutable.ArrayBuffer

class KMeansActor(@transient ctx:RequestContext) extends TrainActor(ctx) {

  val redis = new RedisDB(host,port.toInt)
 
  override def validate(req:ServiceRequest) {
      
    if (req.data.contains("top") == false) 
      throw new Exception("Parameter 'top' is missing.")
        
    if (req.data.contains("iterations") == false)
      throw new Exception("Parameter 'iterations' is missing.")
        
    if (req.data.contains("strategy") == false)
      throw new Exception("Parameter 'strategy' is missing.")
    
  }
  
  override def train(req:ServiceRequest) {
          
    val source = new VectorSource(ctx.sc,ctx.config,new VectorSpec(req))
    val dataset = VectorHandler.vector2LabeledPoints(source.connect(req))
      
    val params = ArrayBuffer.empty[Param]
      
    val top = req.data("top").toInt
    params += Param("top","integer",top.toString)

    val strategy = req.data("strategy").asInstanceOf[String]
    params += Param("strategy","string",strategy)

    val iter = req.data("iterations").toInt
    params += Param("iterations","integer",iter.toString)

    cache.addParams(req, params.toList)
 
    val points = new KMeansDetector().find(dataset,strategy,iter,top).toList          
    savePoints(req,ClusteredPoints(points))
    
  }
  
  private def savePoints(req:ServiceRequest,points:ClusteredPoints) {
    redis.addPoints(req,points)    
  }
  
}