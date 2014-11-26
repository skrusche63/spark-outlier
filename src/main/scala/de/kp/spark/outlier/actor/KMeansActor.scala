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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._

import de.kp.spark.outlier.KMeansDetector
import de.kp.spark.outlier.model._

import de.kp.spark.outlier.source.FeatureSource

import de.kp.spark.outlier.sink.RedisSink

class KMeansActor(@transient val sc:SparkContext) extends BaseActor {

  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
 
        try {

          cache.addStatus(req,OutlierStatus.STARTED)
          
          val dataset = new FeatureSource(sc).get(req)          
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
      
      val k = req.data("k").asInstanceOf[Int]
      val strategy = req.data("strategy").asInstanceOf[String]
        
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
    cache.addStatus(req,OutlierStatus.FINISHED)

   /* Notify potential listeners */
   notify(req,OutlierStatus.FINISHED)
    
  }
  
  private def saveOutliers(req:ServiceRequest,outliers:FOutliers) {
    
    val sink = new RedisSink()
    sink.addFOutliers(req,outliers)
    
  }
  
}