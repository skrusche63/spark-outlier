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

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.outlier.Configuration

import de.kp.spark.outlier.model._
import de.kp.spark.outlier.util.{JobCache,DetectorCache}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class OutlierMiner extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  private val algorithms = Array(Algorithms.KMEANS,Algorithms.MARKOV)
  private val sources = Array(Sources.FILE,Sources.ELASTIC,Sources.JDBC,Sources.PIWIK)
  
  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      val uid = req.data("uid")
      
      req.task match {
        
        case "predict" => {
          
          val response = validate(req.data) match {
            
            case None => train(req).mapTo[ServiceResponse]            
            case Some(message) => Future {failure(req,message)}
            
          }

          response.onSuccess {
            case result => origin ! OutlierModel.serializeResponse(result)
          }

          response.onFailure {
            case throwable => {             
              val resp = failure(req,throwable.toString)
              origin ! OutlierModel.serializeResponse(resp)	                  
           }	  
          }
         
        }
       
        case "status" => {
          
          val resp = if (JobCache.exists(uid) == false) {           
            failure(req,Messages.TASK_DOES_NOT_EXIST(uid))
            
          } else {            
            status(req)
            
          }
           
          origin ! OutlierModel.serializeResponse(resp)
            
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          origin ! OutlierModel.serializeResponse(failure(req,msg))
          
        }
        
      }
      
    }
    
    case _ => {}
  
  }
  
  private def train(req:ServiceRequest):Future[Any] = {

    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second
    
    ask(actor(req), req)
  
  }

  private def status(req:ServiceRequest):ServiceResponse = {
    
    val uid = req.data("uid")
    val data = Map("uid" -> uid)
                
    new ServiceResponse(req.service,req.task,data,JobCache.status(uid))	

  }

  private def validate(params:Map[String,String]):Option[String] = {

    val uid = params("uid")
 
    if (JobCache.exists(uid)) {            
      val message = Messages.TASK_ALREADY_STARTED(uid)
      return Some(message)
    
    }
    
    params.get("algorithm") match {
        
      case None => {
        return Some(Messages.NO_ALGORITHM_PROVIDED(uid))              
      }
        
      case Some(algorithm) => {
        if (algorithms.contains(algorithm) == false) {
          return Some(Messages.ALGORITHM_IS_UNKNOWN(uid,algorithm))    
        }
          
      }
    
    }  
    
    params.get("source") match {
        
      case None => {
        return Some(Messages.NO_SOURCE_PROVIDED(uid))          
      }
        
      case Some(source) => {
        if (sources.contains(source) == false) {
          return Some(Messages.SOURCE_IS_UNKNOWN(uid,source))    
        }          
      }
        
    }
    
    None
    
  }

  private def actor(req:ServiceRequest):ActorRef = {

    val algorithm = req.data("algorithm")
    if (algorithm == Algorithms.KMEANS) {      
      context.actorOf(Props(new KMeansActor()))      
    } else {
     context.actorOf(Props(new MarkovActor()))
    }
  
  }

  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,OutlierStatus.FAILURE)	
    
  }

}