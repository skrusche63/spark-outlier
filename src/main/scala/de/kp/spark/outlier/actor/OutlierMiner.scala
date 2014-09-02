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
import de.kp.spark.outlier.util.{JobCache,OutlierCache}

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class OutlierMiner extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  
  def receive = {
    
    case req:OutlierRequest => {
      
      val origin = sender
      
      val (uid,task) = (req.uid,req.task)
      task match {
        
        case "start" => {
          
          val source = req.source.getOrElse(null)
          val parameters = req.parameters.getOrElse(null)          
          
          val response = validateStart(uid,parameters,source) match {
            
            case None => {
              /* Build job configuration */
              val jobConf = new JobConf()
                
              jobConf.set("uid",uid)
              jobConf.set("k",parameters.k)
               
              parameters.strategy match {
                case None => {}
                case Some(strategy) => jobConf.set("strategy",strategy)
              }
              /* Start job */
              startJob(jobConf,source).mapTo[OutlierResponse]
              
            }
            
            case Some(message) => {
              Future {new OutlierResponse(uid,Some(message),None,OutlierStatus.FAILURE)} 
              
            }
            
          }

          response.onSuccess {
            case result => origin ! OutlierModel.serializeResponse(result)
          }

          response.onFailure {
            case message => {             
              val resp = new OutlierResponse(uid,Some(message.toString),None,OutlierStatus.FAILURE)
              origin ! OutlierModel.serializeResponse(resp)	                  
            }	  
          }
         
        }
       
        case "status" => {
          /*
           * Job MUST exist the return actual status
           */
          val resp = if (JobCache.exists(uid) == false) {           
            val message = OutlierMessages.TASK_DOES_NOT_EXIST(uid)
            new OutlierResponse(uid,Some(message),None,OutlierStatus.FAILURE)
            
          } else {            
            val status = JobCache.status(uid)
            new OutlierResponse(uid,None,None,status)
            
          }
           
          origin ! OutlierModel.serializeResponse(resp)
           
        }
        
        case _ => {
          
          val message = OutlierMessages.TASK_IS_UNKNOWN(uid,task)
          val resp = new OutlierResponse(uid,Some(message),None,OutlierStatus.FAILURE)
           
          origin ! OutlierModel.serializeResponse(resp)
           
        }
        
      }
      
    }
    
    case _ => {}
  
  }
  
  private def startJob(jobConf:JobConf,source:OutlierSource):Future[Any] = {

    val duration = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(duration).second

    val path = source.path.getOrElse(null)
    if (path == null) {
        
      val nodes = source.nodes.getOrElse(null)
      val port  = source.port.getOrElse(null)
        
      val resource = source.resource.getOrElse(null)
      val query = source.query.getOrElse(null)

      val req = new ElasticRequest(nodes,port,resource,query)
      val actor = context.actorOf(Props(new OutlierActor(jobConf)))
      
      ask(actor, req)
        
    } else {
    
      val req = new FileRequest(path)
      val actor = context.actorOf(Props(new OutlierActor(jobConf)))

      ask(actor, req)
        
    }
  
  }

  private def validateStart(uid:String,parameters:OutlierParameters,source:OutlierSource):Option[String] = {

    if (JobCache.exists(uid)) {            
      val message = OutlierMessages.TASK_ALREADY_STARTED(uid)
      return Some(message)
    
    }
    
    if (parameters == null) {
      val message = OutlierMessages.NO_PARAMETERS_PROVIDED(uid)
      return Some(message)
      
    }
    
    if (source == null) {
      val message = OutlierMessages.NO_SOURCE_PROVIDED(uid)
      return Some(message)
 
    }

    None
    
  }

}