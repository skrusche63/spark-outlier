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

import de.kp.spark.core.model._

import de.kp.spark.outlier.RequestContext
import de.kp.spark.outlier.model._

class TrainActor(@transient ctx:RequestContext) extends BaseActor {

  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender
      val missing = try {
        
        validate(req)
        false
      
      } catch {
        case e:Exception => true
        
      }

      origin ! response(req, missing)

      if (missing == false) {
 
        try {

          /* Update cache */
          cache.addStatus(req,OutlierStatus.TRAINING_STARTED)
          
          train(req)
          
          /* Update cache */
          cache.addStatus(req,OutlierStatus.TRAINING_FINISHED)
 
        } catch {
          case e:Exception => cache.addStatus(req,OutlierStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("unknown request.")
      context.stop(self)
      
    }
    
  }
  
  protected def validate(req:ServiceRequest) = {}
  
  protected def train(req:ServiceRequest) {}

}