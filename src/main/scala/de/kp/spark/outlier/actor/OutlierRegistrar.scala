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

import de.kp.spark.core.Names
import de.kp.spark.core.spec.FieldBuilder

import de.kp.spark.core.model._
import de.kp.spark.outlier.model._

import scala.collection.mutable.ArrayBuffer

class OutlierRegistrar extends BaseActor {

  implicit val ec = context.dispatcher

  def receive = {
    
    case req:ServiceRequest => {
      
      val origin = sender
      val uid = req.data(Names.REQ_UID)
      
      val response = try {

        req.task.split(":")(1) match {
        
          case "feature" => {
        
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
 
            cache.addFields(req, fields.toList)        
            new ServiceResponse(req.service,req.task,Map(Names.REQ_UID-> uid),OutlierStatus.SUCCESS)
         
          } 
        
          case "sequence" => {
        
            val fields = new FieldBuilder().build(req,"product")
            cache.addFields(req, fields)
        
            new ServiceResponse(req.service,req.task,Map(Names.REQ_UID-> uid),OutlierStatus.SUCCESS)
          
          }
        
          case _ => {
          
            val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
            failure(req,msg)
          
          }
        
        }
        
      } catch {
        case throwable:Throwable => failure(req,throwable.getMessage)
      }
      
      origin ! response
      context.stop(self)      
      
    }
  
  }
}