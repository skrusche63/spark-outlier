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

import de.kp.spark.core.model._

import de.kp.spark.outlier.model._
import de.kp.spark.outlier.sink.RedisSink

class OutlierQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  private val sink = new RedisSink()

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      val uid = req.data("uid")
      
      req.task match {
        
        case "get:sequence" => {

          val response = {

            if (sink.behaviorExists(req) == false) {   
              failure(req,Messages.OUTLIERS_DO_NOT_EXIST(uid))
            
            } else {       
                
              /* Retrieve and serialize detected outliers */
              val outliers = sink.behavior(req)

              val data = Map("uid" -> uid, "sequence" -> outliers)            
              new ServiceResponse(req.service,req.task,data,OutlierStatus.SUCCESS)
            
            }
          } 
          
          origin ! response
          context.stop(self)
          
        }
        case "get:feature" => {

          val response = {

            if (sink.featuresExist(req) == false) {    
              failure(req,Messages.OUTLIERS_DO_NOT_EXIST(uid))
            
            } else {         
                
              /* Retrieve and serialize detected outliers */
                val outliers = sink.features(req)

              val data = Map("uid" -> uid, "feature" -> outliers)            
              new ServiceResponse(req.service,req.task,data,OutlierStatus.SUCCESS)
             
            }
          
          } 
          origin ! response
          context.stop(self)
           
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          origin ! failure(req,msg)
          context.stop(self)
          
        }
        
      }
      
    }
  
  }
 
}