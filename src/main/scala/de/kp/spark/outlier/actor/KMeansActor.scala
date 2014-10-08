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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import de.kp.spark.outlier.KMeansDetector
import de.kp.spark.outlier.model._

import de.kp.spark.outlier.source.FeatureSource
import de.kp.spark.outlier.redis.RedisCache

class KMeansActor(@transient val sc:SparkContext) extends Actor with ActorLogging {

  def receive = {

    case req:ServiceRequest => {

      val uid = req.data("uid")   
      val task = req.task
      
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
 
        try {

          RedisCache.addStatus(uid,task,OutlierStatus.STARTED)
          
          val dataset = new FeatureSource(sc).get(req.data)          
          findOutliers(uid,task,dataset,params)

        } catch {
          case e:Exception => RedisCache.addStatus(uid,task,OutlierStatus.FAILURE)          
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
  
  private def findOutliers(uid:String,task:String,dataset:RDD[LabeledPoint],params:(Int,String)) {

    RedisCache.addStatus(uid,task,OutlierStatus.DATASET)
    
    /* Find outliers in set of labeled datapoints */
    val (k,strategy) = params     
    val outliers = new KMeansDetector().find(dataset,strategy,100,k).toList
          
    /* Put outliers to cache */
    RedisCache.addFOutliers(uid,new FOutliers(outliers))
          
    /* Update cache */
    RedisCache.addStatus(uid,task,OutlierStatus.FINISHED)
    
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,OutlierStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.OUTLIER_DETECTION_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,OutlierStatus.STARTED)	
      
  
    }

  }
  
}