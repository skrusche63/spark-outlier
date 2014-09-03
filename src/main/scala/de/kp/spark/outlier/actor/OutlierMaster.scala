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

import akka.actor.{OneForOneStrategy, SupervisorStrategy}
import akka.routing.RoundRobinRouter

import de.kp.spark.outlier.Configuration
import de.kp.spark.outlier.model._

import scala.concurrent.duration.DurationInt
import scala.concurrent.Future

class OutlierMaster extends Actor with ActorLogging {
  
  /* Load configuration for routers */
  val (time,retries,workers) = Configuration.router   

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries=retries,withinTimeRange = DurationInt(time).minutes) {
    case _ : Exception => SupervisorStrategy.Restart
  }

  val miner = context.actorOf(Props[OutlierMiner])
  val questor = context.actorOf(Props[OutlierQuestor].withRouter(RoundRobinRouter(workers)))

  def receive = {
    
    case req:String => {
      
      implicit val ec = context.dispatcher

      val duration = Configuration.actor      
      implicit val timeout:Timeout = DurationInt(duration).second
	  	    
	  val origin = sender
	  val deser = OutlierModel.deserializeRequest(req)
	  val (uid,task) = (deser.uid,deser.task)

	  val response = deser.task match {
        
        case "start" => ask(miner,deser).mapTo[OutlierResponse]
        case "status" => ask(miner,deser).mapTo[OutlierResponse]
        
        case "outlier" => ask(questor,deser).mapTo[OutlierResponse]
       
        case _ => {

          Future {          
            val message = OutlierMessages.TASK_IS_UNKNOWN(uid,task)
            new OutlierResponse(uid,Some(message),None,None,OutlierStatus.FAILURE)
          } 
        }
      
      }
      response.onSuccess {
        case result => origin ! OutlierModel.serializeResponse(result)
      }
      response.onFailure {
        case result => origin ! OutlierStatus.FAILURE	      
	  }
      
    }
  
    case _ => {}
    
  }

}