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

import de.kp.spark.outlier.model._

import de.kp.spark.outlier.MarkovDetector
import de.kp.spark.outlier.markov.TransitionMatrix

import de.kp.spark.outlier.source.{BehaviorSource}
import de.kp.spark.outlier.redis.RedisCache

import de.kp.spark.outlier.sink.RedisSink

class MarkovActor(@transient val sc:SparkContext) extends BaseActor {

  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
        /* Register status */
        RedisCache.addStatus(req,OutlierStatus.STARTED)
 
        try {
          
          val dataset = new BehaviorSource(sc).get(req.data)
          findOutliers(req,dataset,params)

        } catch {
          case e:Exception => RedisCache.addStatus(req,OutlierStatus.FAILURE)          
        }
 

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("Unkonw request.")
      context.stop(self)
      
    }
    
  }
  
  private def properties(req:ServiceRequest):(String,Double) = {
      
    try {
      
      val threshold = req.data("threshold").asInstanceOf[Double]
      val strategy  = req.data("algorithm").asInstanceOf[String]
         
      return (strategy,threshold)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
    
  private def findOutliers(req:ServiceRequest,sequences:RDD[Behavior],params:(String,Double)) {

    RedisCache.addStatus(req,OutlierStatus.DATASET)

    val detector = new MarkovDetector()
    
    val model = detector.train(sequences)
    RedisCache.addStatus(req,OutlierStatus.TRAINED)
         
    val (algorithm,threshold) = params          
    val outliers = detector.detect(sequences,algorithm,threshold,model).collect().toList
          
    saveOutliers(req,new BOutliers(outliers))
          
    /* Update cache */
    RedisCache.addStatus(req,OutlierStatus.FINISHED)

    /* Notify potential listeners */
    notify(req,OutlierStatus.FINISHED)
    
  }
  
  private def saveOutliers(req:ServiceRequest,outliers:BOutliers) {
    
    val sink = new RedisSink()
    sink.addBOutliers(req,outliers)
    
  }

}