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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._
import de.kp.spark.outlier.markov.{MarkovBuilder,StateMetrics,TransitionMatrix}

/**
 * The MarkovDetector discovers outliers from registered behavior.
 */
class MarkovDetector(@transient ctx:RequestContext,scale:Int,states:Array[String]) extends Serializable {

  val metrics  = new StateMetrics(states)
  
  def detect(sequences:RDD[Behavior],algorithm:String,threshold:Double,matrix:TransitionMatrix):RDD[Outlier] = {

    val bmatrix = ctx.sc.broadcast(matrix)    
    sequences.map(seq => {
      
      val (site,user,states) = (seq.site,seq.user,seq.states)
      val metric = algorithm match {
        
        case "missprob" => metrics.missProbMetric(states,bmatrix.value)
        
        case "missrate" => metrics.missRateMetric(states,bmatrix.value)
        
        case "entreduc" => metrics.entropyReductionMetric(states,bmatrix.value)
        
      }
      
      val flag = if (metric > threshold) "yes" else "no"      
      Outlier(site,user,states,metric,flag)
      
    })
    
  }

  def train(sequences:RDD[Behavior]):TransitionMatrix = {
    new MarkovBuilder(scale,states).build(sequences)
  }
  
}