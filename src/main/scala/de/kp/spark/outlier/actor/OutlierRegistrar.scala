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

import de.kp.spark.outlier.model._
import de.kp.spark.outlier.redis.RedisCache

import scala.collection.mutable.ArrayBuffer

class OutlierRegistrar extends BaseActor {

  implicit val ec = context.dispatcher

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      val uid = req.data("uid")
      
      req.task match {
        
        case "register:features" => {
      
          val response = try {
        
            /* Unpack fields from request and register in Redis instance */
            val fields = ArrayBuffer.empty[Field]

            /*
             * ********************************************
             * 
             *  "uid" -> 123
             *  "names" -> "target,feature,feature,feature"
             *  "types" -> "string,double,double,double"
             *
             * ********************************************
             * 
             * It is important to have the names specified in the order
             * they are used (later) to retrieve the respective data
             */
            val names = req.data("names").split(",")
            val types = req.data("types").split(",")
        
            val zip = names.zip(types)
        
            val target = zip.head
            if (target._2 != "string") throw new Exception("Target variable must be a String")
        
            fields += new Field(target._1,target._2,"")
        
            for (feature <- zip.tail) {
          
              if (feature._2 != "double") throw new Exception("A feature must be a Double.")          
              fields += new Field(feature._1,"double","")
        
            }
 
            RedisCache.addFields(req, new Fields(fields.toList))
        
            new ServiceResponse("outlier","register",Map("uid"-> uid),OutlierStatus.SUCCESS)
        
          } catch {
            case throwable:Throwable => failure(req,throwable.getMessage)
          }
      
          origin ! Serializer.serializeResponse(response)
          
        } 
        
        case "register:sequences" => {
      
          val response = try {
        
            /* Unpack fields from request and register in Redis instance */
            val fields = ArrayBuffer.empty[Field]

            fields += new Field("site","string",req.data("site"))
            fields += new Field("timestamp","long",req.data("timestamp"))

            fields += new Field("user","string",req.data("user"))
            fields += new Field("group","string",req.data("group"))

            fields += new Field("item","integer",req.data("integer"))
            fields += new Field("price","float",req.data("price"))
            
            RedisCache.addFields(req, new Fields(fields.toList))
        
            new ServiceResponse("outlier","register",Map("uid"-> uid),OutlierStatus.SUCCESS)
        
          } catch {
            case throwable:Throwable => failure(req,throwable.getMessage)
          }
      
          origin ! Serializer.serializeResponse(response)
          
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          origin ! Serializer.serializeResponse(failure(req,msg))
          context.stop(self)
          
        }
        
      }
    }
  
  }
}