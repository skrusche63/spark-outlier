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

import akka.actor.{ActorRef,Props}

import akka.pattern.ask
import akka.util.Timeout

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.outlier.Configuration

import de.kp.spark.outlier.model._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
 * The focus of the OutlierMiner is on the model building task,
 * either for cluster analysis based tasks or markov based states.
 */
class OutlierMiner(@transient sc:SparkContext) extends BaseActor {

  implicit val ec = context.dispatcher
  
  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      /*
       * The requests initiates the generation of an outlier model, that
       * is either represented as the result of a cluster analysis of a
       * certain feature set or as a transition matrix of customer states. 
       */
      val response = try {

        validate(req) match {
            
          case None => train(req).mapTo[ServiceResponse]            
          case Some(message) => Future {failure(req,message)}
            
        }
        
      } catch {
        case e:Exception => Future {failure(req,e.getMessage)}
      }

      response.onSuccess {
        case result => {
              
          origin ! result
          context.stop(self)
              
        }
      }

      response.onFailure {
        case throwable => {       
          
          origin ! failure(req,throwable.toString)	                  
          context.stop(self)
              
        }	  
          
      }
      
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))
      context.stop(self)

    }
  
  }
  
  private def train(req:ServiceRequest):Future[Any] = {

    val (duration,retries,time) = Configuration.actor      
    implicit val timeout:Timeout = DurationInt(time).second
    
    ask(actor(req), req)
  
  }

  private def validate(req:ServiceRequest):Option[String] = {

    val uid = req.data(Names.REQ_UID)
 
    if (cache.statusExists(req)) {            
      val message = Messages.TASK_ALREADY_STARTED(uid)
      return Some(message)
    
    }
    
    req.data.get(Names.REQ_ALGORITHM) match {
        
      case None => {
        return Some(Messages.NO_ALGORITHM_PROVIDED(uid))              
      }
        
      case Some(algorithm) => {
        if (Algorithms.isAlgorithm(algorithm) == false) {
          return Some(Messages.ALGORITHM_IS_UNKNOWN(uid,algorithm))    
        }
          
      }
    
    }  
    
    req.data.get("source") match {
        
      case None => {
        return Some(Messages.NO_SOURCE_PROVIDED(uid))          
      }
        
      case Some(source) => {
        if (Sources.isSource(source) == false) {
          return Some(Messages.SOURCE_IS_UNKNOWN(uid,source))    
        }          
      }
        
    }
    
    None
    
  }

  /**
   * This is a helper method to determine which actor has to be
   * created to support the requested algorithm; actually KMeans
   * and Markov based algorithms are supported.
   */
  private def actor(req:ServiceRequest):ActorRef = {

    val algorithm = req.data(Names.REQ_ALGORITHM)
    if (algorithm == Algorithms.KMEANS) {      
      context.actorOf(Props(new KMeansActor(sc)))      
    
    } else {
     context.actorOf(Props(new MarkovActor(sc)))
    
    }
  
  }

}