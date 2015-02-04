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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.source.StateSource
import de.kp.spark.core.source.handler.StateHandler

import de.kp.spark.outlier.Configuration
import de.kp.spark.outlier.model._

import de.kp.spark.outlier.MarkovDetector
import de.kp.spark.outlier.markov.TransitionMatrix

import de.kp.spark.outlier.sink.RedisSink
import de.kp.spark.outlier.spec.StateSpec

class MarkovActor(@transient sc:SparkContext) extends BaseActor {

  private val config = Configuration
  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)
      val missing = (params == null)

      sender ! response(req, missing)

      if (missing == false) {
        /* Register status */
        cache.addStatus(req,OutlierStatus.STARTED)
 
        try {
          
          val source = new StateSource(sc,config,StateSpec)          
          val dataset = StateHandler.state2Behavior(source.connect(req))
          
          findOutliers(req,dataset,params)

        } catch {
          case e:Exception => cache.addStatus(req,OutlierStatus.FAILURE)          
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
      
      val threshold = req.data(Names.REQ_THRESHOLD).asInstanceOf[Double]
      val strategy  = req.data(Names.REQ_STRATEGY).asInstanceOf[String]
         
      return (strategy,threshold)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
    
  private def findOutliers(req:ServiceRequest,sequences:RDD[Behavior],params:(String,Double)) {

    cache.addStatus(req,OutlierStatus.DATASET)

    val scale = req.data(Names.REQ_SCALE).toInt
    val states = req.data(Names.REQ_STATES).split(",")

    val detector = new MarkovDetector(sc,scale,states)
    
    val model = detector.train(sequences)
    cache.addStatus(req,OutlierStatus.TRAINED)
         
    val (strategy,threshold) = params          
    val outliers = detector.detect(sequences,strategy,threshold,model).collect().toList
          
    saveOutliers(req,new BOutliers(outliers))
          
    /* Update cache */
    cache.addStatus(req,OutlierStatus.FINISHED)

    /* Notify potential listeners */
    notify(req,OutlierStatus.FINISHED)
    
  }
  
  private def saveOutliers(req:ServiceRequest,outliers:BOutliers) {
    
    val sink = new RedisSink()
    sink.addBOutliers(req,outliers)
    
  }

}