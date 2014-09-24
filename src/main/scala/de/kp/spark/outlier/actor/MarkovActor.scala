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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}
import org.apache.spark.rdd.RDD

import de.kp.spark.outlier.model._

import de.kp.spark.outlier.{Configuration,MarkovDetector}
import de.kp.spark.outlier.markov.TransitionMatrix

import de.kp.spark.outlier.source.{BehaviorSource}
import de.kp.spark.outlier.util.{JobCache,BehaviorCache}

class MarkovActor extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("MarkovActor",Configuration.spark)      

  def receive = {

    case req:ServiceRequest => {

      val uid = req.data("uid")     
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
        /* Register status */
        JobCache.add(uid,OutlierStatus.STARTED)
 
        try {
          
          val dataset = new BehaviorSource(sc).get(req.data("source"))
          findOutliers(uid,dataset,params)

        } catch {
          case e:Exception => JobCache.add(uid,OutlierStatus.FAILURE)          
        }
 

      }
      
      sc.stop
      context.stop(self)
          
    }
    
    case _ => {
      
      sc.stop
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
    
  private def findOutliers(uid:String,sequences:RDD[Behavior],params:(String,Double)) {

    JobCache.add(uid,OutlierStatus.DATASET)

    val detector = new MarkovDetector()
    
    val model = detector.train(sequences)
    JobCache.add(uid,OutlierStatus.TRAINED)
         
    val (algorithm,threshold) = params          
    val outliers = detector.detect(sequences,algorithm,threshold,model).collect().toList
          
    /* Put outliers to BehaviorCache */
    BehaviorCache.add(uid,outliers)
          
    /* Update JobCache */
    JobCache.add(uid,OutlierStatus.FINISHED)
    
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