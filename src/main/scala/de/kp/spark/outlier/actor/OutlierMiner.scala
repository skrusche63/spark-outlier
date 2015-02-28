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

import akka.actor.{ActorRef,Props}

import de.kp.spark.core.Names

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.outlier.{Configuration,RequestContext}
import de.kp.spark.outlier.model._

/**
 * The focus of the OutlierMiner is on the model building task,
 * either for cluster analysis based tasks or markov based states.
 */
class OutlierMiner(@transient ctx:RequestContext) extends BaseTrainer(Configuration) {

  protected def validate(req:ServiceRequest):Option[String] = {

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
    
    req.data.get(Names.REQ_SOURCE) match {
        
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
  protected def actor(req:ServiceRequest):ActorRef = {

    val algorithm = req.data(Names.REQ_ALGORITHM)
    if (algorithm == Algorithms.KMEANS) {      
      context.actorOf(Props(new KMeansActor(ctx)))      
    
    } else {
     context.actorOf(Props(new MarkovActor(ctx)))
    
    }
  
  }

}