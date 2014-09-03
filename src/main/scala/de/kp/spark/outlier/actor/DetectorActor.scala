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

import org.apache.hadoop.conf.{Configuration => HConf}

import org.apache.spark.rdd.RDD

import de.kp.spark.outlier.OutlierDetector
import de.kp.spark.outlier.model._

import de.kp.spark.outlier.source.{ElasticSource,FileSource}
import de.kp.spark.outlier.util.{JobCache,OutlierCache}

class DetectorActor(jobConf:JobConf) extends Actor with SparkActor {
   
  /* Specification of Spark specific system properties */
  private val props = Map(
    "spark.executor.memory"          -> "1g",
	"spark.kryoserializer.buffer.mb" -> "256"
  )
  
  /* Create Spark context */
  private val sc = createCtxLocal("OutlierActor",props)      
  
  private val uid = jobConf.get("uid").get.asInstanceOf[String]     
  JobCache.add(uid,OutlierStatus.STARTED)

  private val params = parameters()

  private val response = if (params == null) {
    val message = OutlierMessages.MISSING_PARAMETERS(uid)
    new OutlierResponse(uid,Some(message),None,OutlierStatus.FAILURE)
  
  } else {
     val message = OutlierMessages.OUTLIER_DETECTION_STARTED(uid)
     new OutlierResponse(uid,Some(message),None,OutlierStatus.STARTED)
    
  }

  def receive = {

    case req:ElasticRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
          
          /* Retrieve data from Elasticsearch */    
          val source = new ElasticSource(sc)
          
          val (nodes,port,resource,query) = (req.nodes,req.port,req.resource,req.query)
    
          /* Elasticsearch specific configuration */
          val conf = new HConf()                          

          conf.set("es.nodes",nodes)
          conf.set("es.port",port)
    
          conf.set("es.resource", resource)                
          conf.set("es.query", query)                          

          val dataset = source.connect(conf)

          JobCache.add(uid,OutlierStatus.DATASET)
          
          val (k,strategy) = params     
          findOutliers(dataset,k,strategy)

        } catch {
          case e:Exception => JobCache.add(uid,OutlierStatus.FAILURE)          
        }
      
      }
      
      sc.stop
      context.stop(self)
      
    }

    case req:FileRequest => {

      /* Send response to originator of request */
      sender ! response
          
      if (params != null) {

        try {
    
          /* Retrieve data from the file system */
          val source = new FileSource(sc)
          
          val path = req.path
          val dataset = source.connect(path)

          JobCache.add(uid,OutlierStatus.DATASET)

          val (k,strategy) = params          
          findOutliers(dataset,k,strategy)

        } catch {
          case e:Exception => JobCache.add(uid,OutlierStatus.FAILURE)
        }
        
      }
      
      sc.stop
      context.stop(self)
      
    }
    
    case _ => {}
    
  }
  
  private def parameters():(Int,String) = {
      
    try {
      
      val k = jobConf.get("k").get.asInstanceOf[Int]
      val strategy = jobConf.get("strategy") match {
        case None => "entropy"
        case Some(strategy) => strategy.asInstanceOf[String]
      }
        
      return (k,strategy)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
  private def findOutliers(dataset:RDD[LabeledPoint],k:Int,strategy:String) {
          
    val outliers = OutlierDetector.find(dataset,strategy,100,k).toList
          
    /* Put outliers to OutlierCache */
    OutlierCache.add(uid,outliers)
          
    /* Update JobCache */
    JobCache.add(uid,OutlierStatus.FINISHED)
    
  }
  
}