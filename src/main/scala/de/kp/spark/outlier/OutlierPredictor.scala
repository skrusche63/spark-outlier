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

import de.kp.spark.outlier.model._
import de.kp.spark.outlier.markov.{MarkovModel,SequenceMetric,StateModel,TransitionMatrix}

import scala.collection.mutable.ArrayBuffer

object OutlierPredictor extends Serializable {

  def predict(sequences:RDD[StateSequence],algorithm:String,threshold:Double,matrix:TransitionMatrix) {

    val sc = sequences.context
    val bmatrix = sc.broadcast(matrix)
    
    sequences.map(seq => {
      
      val (site,user,states) = (seq.site,seq.user,seq.states)
      val metric = algorithm match {
        
        case "missprob" => SequenceMetric.missProbMetric(states,bmatrix.value)
        
        case "missrate" => SequenceMetric.missRateMetric(states,bmatrix.value)
        
        case "entreduc" => SequenceMetric.entropyReductionMetric(states,bmatrix.value)
        
      }
      
      val flag = if (metric > threshold) "yes" else "no"      
      (site,user,metric,flag)
      
    })
    
  }

  def train(sequences:RDD[StateSequence]):TransitionMatrix = {
    MarkovModel.buildTransProbs(sequences)
  }

  /*
   * Format: (site,user,order,timestamp,item,price)
   */
  def prepare(source:RDD[(String,String,String,Long,String,Float)],params:Map[String,String]):RDD[StateSequence] = {
    
    /*
     * Group source by 'order' and aggregate all items of a 
     * single order into a single transaction
     */
    val transactions = source.groupBy(_._3).map(valu => {
      
      /* Sort grouped orders by (ascending) timestamp */
      val data = valu._2.toList.sortBy(_._4)      
      val items = ArrayBuffer.empty[CommerceItem]
      
      val (site,user,order,timestamp,item,price) = data.head
      items += new CommerceItem(item,price)
      
      for (rec <- data.tail) {
        items += new CommerceItem(rec._5,rec._6)
      }
      
      new CommerceTransaction(site,user,order,timestamp,items.toList)
      
    })

    /* Transform commerce transactions into state sequences */
    StateModel.build(transactions, params)
    
  }
  
}