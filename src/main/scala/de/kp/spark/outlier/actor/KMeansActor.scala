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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.outlier.{Configuration,KMeansDetector,RequestContext}
import de.kp.spark.outlier.model._

import de.kp.spark.core.source.VectorSource
import de.kp.spark.core.source.handler.VectorHandler

import de.kp.spark.outlier.sink.RedisSink
import de.kp.spark.outlier.spec.VectorSpec

class KMeansActor(@transient ctx:RequestContext) extends BaseActor {

  private val config = Configuration
  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
 
        try {

          cache.addStatus(req,OutlierStatus.TRAINING_STARTED)
          
          val source = new VectorSource(ctx.sc,config,new VectorSpec(req))
          val dataset = VectorHandler.vector2LabeledPoints(source.connect(req))
          
          findOutliers(req,dataset,params)

        } catch {
          case e:Exception => cache.addStatus(req,OutlierStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def properties(req:ServiceRequest):(Int,String) = {
      
    try {
      
      val k = req.data(Names.REQ_K).asInstanceOf[Int]
      val strategy = req.data(Names.REQ_STRATEGY).asInstanceOf[String]
        
      return (k,strategy)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
  private def findOutliers(req:ServiceRequest,dataset:RDD[LabeledPoint],params:(Int,String)) {

    cache.addStatus(req,OutlierStatus.DATASET)
    
    /* Find outliers in set of labeled datapoints */
    val (k,strategy) = params     
    val outliers = new KMeansDetector().find(dataset,strategy,100,k).toList
          
    saveOutliers(req,new FOutliers(outliers))
          
    /* Update cache */
    cache.addStatus(req,OutlierStatus.TRAINING_FINISHED)

   /* Notify potential listeners */
   notify(req,OutlierStatus.TRAINING_FINISHED)
    
  }
  
  private def saveOutliers(req:ServiceRequest,outliers:FOutliers) {
    
    val sink = new RedisSink()
    sink.addFOutliers(req,outliers)
    
  }
  
}