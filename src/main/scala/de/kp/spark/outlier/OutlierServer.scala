package de.kp.spark.outlier
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

import akka.actor.{ActorSystem,Props}
import com.typesafe.config.ConfigFactory

import de.kp.spark.core.SparkService
import de.kp.spark.outlier.api.AkkaApi

/**
 * The OutlierServer supports two different approaches to outlier discovery; one is based 
 * on clustering analysis and determines outlier feature sets due to their distance to the
 * cluster centers. This approach is independent of a certain use case and concentrates on
 * the extraction and evaluation of (equal-size) feature vectors. The other approach to 
 * outlier discovery has a strong focus on the customers purchase behavior and detects those
 * customer that behave different from all other customers.
 */
object OutlierServer extends SparkService {
  
  private val sc = createCtxLocal("IntentContext",Configuration.spark)      

  def main(args: Array[String]) {
    
    /**
     * AKKA API 
     */
    val conf:String = "server.conf"

    val akkaSystem = ActorSystem("akka-server",ConfigFactory.load(conf))
    sys.addShutdownHook(akkaSystem.shutdown)
    
    new AkkaApi(akkaSystem,sc).start()
 
    println("AKKA API activated.")
      
  }

}