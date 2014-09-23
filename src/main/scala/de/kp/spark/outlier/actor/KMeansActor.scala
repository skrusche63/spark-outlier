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

import de.kp.spark.outlier.{Configuration,OutlierDetector}
import de.kp.spark.outlier.model._

import de.kp.spark.outlier.source.{ElasticSource,FileSource}
import de.kp.spark.outlier.util.{JobCache,DetectorCache}

class KMeansActor extends Actor with SparkActor {
  
  /* Create Spark context */
  private val sc = createCtxLocal("KMeansActor",Configuration.spark)      

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
          
          val source = req.data("source")
          val dataset = source match {
            
            /* 
             * Discover outliers from feature set persisted as an appropriate search 
             * index from Elasticsearch; the configuration parameters are retrieved 
             * from the service configuration 
             */    
            case Sources.ELASTIC => new ElasticSource(sc).features
            /* 
             * Discover outliers from feature set persisted as a file on the (HDFS) 
             * file system; the configuration parameters are retrieved from the service 
             * configuration  
             */    
            case Sources.FILE => new FileSource(sc).features
            /*
             * Discover outliers from feature set persisted as an appropriate table 
             * from a JDBC database; the configuration parameters are retrieved from 
             * the service configuration
             */
            //case Sources.JDBC => new JdbcSource(sc).connect(req.data)
             /*
             * Discover outliers from feature set persisted as an appropriate table 
             * from a Piwik database; the configuration parameters are retrieved from 
             * the service configuration
             */
            //case Sources.PIWIK => new PiwikSource(sc).connect(req.data)
            
          }

          JobCache.add(uid,OutlierStatus.DATASET)
          
          val (k,strategy) = params     
          findOutliers(uid,dataset,k,strategy)

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
  
  private def findOutliers(uid:String,dataset:RDD[LabeledPoint],k:Int,strategy:String) {
          
    val outliers = OutlierDetector.find(dataset,strategy,100,k).toList
          
    /* Put outliers to DetectorCache */
    DetectorCache.add(uid,outliers)
          
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