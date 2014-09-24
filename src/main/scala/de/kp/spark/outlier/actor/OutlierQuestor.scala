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

import de.kp.spark.outlier.{Detection,Feature,Prediction}

import de.kp.spark.outlier.model._
import de.kp.spark.outlier.util.{FeatureCache,BehaviorCache}

import de.kp.spark.outlier.spec.FeatureSpec
import scala.collection.mutable.ArrayBuffer

class OutlierQuestor extends Actor with ActorLogging {

  implicit val ec = context.dispatcher
  val spec = FeatureSpec.get

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      val uid = req.data("uid")
      
      req.task match {
        
        case "get" => {

          val algorithm = req.data("algorithm")
          val response = algorithm match {
            
            case Algorithms.KMEANS => {

              if (FeatureCache.exists(uid) == false) {    
                failure(req,Messages.OUTLIERS_DO_NOT_EXIST(uid))
            
              } else {         
                
                /* Retrieve and serialize detected outliers */
                val outliers = FeatureCache.outliers(uid).map(outlier => {
                  
                  val (distance,point) = outlier
                  val (label,values) = (point.label,point.features)
                  
                  val features = ArrayBuffer.empty[Feature]
                  (1 until spec.length).foreach(i => {
                    
                    val name  = spec(i)
                    val value = values(i-1)
                    
                    features += new Feature(name,value)
                  
                  })
                  
                  new Detection(distance,label,features.toList).toJSON
                  
                }).mkString(",")

                val data = Map("uid" -> uid, "outliers" -> outliers)            
                new ServiceResponse(req.service,req.task,data,OutlierStatus.SUCCESS)
             
              }
              
            }
            
            case Algorithms.MARKOV => {

              if (BehaviorCache.exists(uid) == false) {   
                failure(req,Messages.OUTLIERS_DO_NOT_EXIST(uid))
            
              } else {       
                
                /* Retrieve and serialize predicted outliers */
                val outliers = BehaviorCache.outliers(uid).map(o => Prediction(o._1,o._2,o._3,o._4,o._5).toJSON).mkString(",")

                val data = Map("uid" -> uid, "outliers" -> outliers)            
                new ServiceResponse(req.service,req.task,data,OutlierStatus.SUCCESS)
            
              }
              
            }
            
            case _ => {
             failure(req,Messages.METHOD_NOT_SUPPORTED(uid))              
            }
            
          }
           
          origin ! OutlierModel.serializeResponse(response)
           
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          origin ! OutlierModel.serializeResponse(failure(req,msg))
           
        }
        
      }
      
    }
  
  }
 
  private def failure(req:ServiceRequest,message:String):ServiceResponse = {
    
    val data = Map("uid" -> req.data("uid"), "message" -> message)
    new ServiceResponse(req.service,req.task,data,OutlierStatus.FAILURE)	
    
  }
 
}