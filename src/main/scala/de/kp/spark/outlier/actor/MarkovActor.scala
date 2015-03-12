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

import de.kp.spark.core.source.StateSource
import de.kp.spark.core.source.handler.StateHandler

import de.kp.spark.core.redis.RedisDB

import de.kp.spark.outlier.RequestContext
import de.kp.spark.outlier.model._

import de.kp.spark.outlier.MarkovDetector
import de.kp.spark.outlier.spec.StateSpec

import scala.collection.mutable.ArrayBuffer

class MarkovActor(@transient ctx:RequestContext) extends TrainActor(ctx) {

  val redis = new RedisDB(host,port.toInt)
  
  override def validate(req:ServiceRequest) {
        
    if (req.data.contains("scale") == false)
      throw new Exception("Parameter 'scale' is missing.")
        
    if (req.data.contains("states") == false)
      throw new Exception("Parameter 'states' is missing.")
        
    if (req.data.contains("strategy") == false)
      throw new Exception("Parameter 'strategy' is missing.")
    
    if (req.data.contains("threshold") == false) 
      throw new Exception("Parameter 'threshold' is missing.")
    
  }
    
  override def train(req:ServiceRequest) {
          
    val source = new StateSource(ctx.sc,ctx.config,new StateSpec(req))          
    val sequences = StateHandler.state2Behavior(source.connect(req))

    val scale = req.data(Names.REQ_SCALE).toInt
    val states = req.data(Names.REQ_STATES).split(",")

    val detector = new MarkovDetector(ctx,scale,states)
    
    val model = detector.train(sequences)
      
    val params = ArrayBuffer.empty[Param]
      
    val strategy  = req.data("strategy")
    params += Param("strategy","string",strategy)

    val threshold = req.data("threshold").toDouble
    params += Param("threshold","double",threshold.toString)

    cache.addParams(req, params.toList)
         
    val outliers = detector.detect(sequences,strategy,threshold,model).collect().toList
          
    saveOutliers(req,new Outliers(outliers))
    
  }
  
  private def saveOutliers(req:ServiceRequest,outliers:Outliers) {
    redis.addOutliers(req,outliers)
  }

}