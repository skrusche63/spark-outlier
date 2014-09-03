package de.kp.spark.outlier.markov
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

import de.kp.spark.outlier.Configuration
import de.kp.spark.outlier.model._

import scala.collection.mutable.ArrayBuffer

object StateModel {
  
  val FD_SCALE = 1
  /*
   * The state model comprises 18 states, which are built from the combination
   * of the individual states from amount, price and elasped time: APT model
   */
  val FD_STATE_DEFS = Array("LNL","LNN","LNS","LHL","LHN","LHS","MNL","MNN","MNS","MHL","MHN","MHS","HNL","HNN","HNS","HHL","HHN","HHS")

  def build(dataset:RDD[CommerceTransaction]):RDD[StateSequence] = {
    new StateModel().buildStateSeq(dataset)    
  }
  
}

/**
 * The StateModel holds the business logic for the markov chain based outlier prediction approach; 
 * it takes 3 different parameters, amount, price and time elasped since last transaction into account 
 * and transforms an ecommerce transaction into a discrete state 
 */
class StateModel() {
  
  private val model = Configuration.model
  /*
   * The parameters amount high, and norm specify threshold
   * value to determine, whether the amount of a transaction 
   * is classified as Low, Normal or High
   */
  private val AMOUNT_HIGH:Float = model("amount.height").toFloat
  private val AMOUNT_NORM:Float = model("amount.norm").toFloat
  /*
   * The parameter price high specifies, whether the transaction
   * holds a high price item
   */
  private val PRICE_HIGH:Float = model("price.high").toFloat
  /*
   * The parameters date small, and medium specify the days elapsed
   * since the last ecommerce transaction, which are classified as
   * Small, Medium, and Large
   */
  private val DATE_SMALL:Int  = model("date.small").toInt
  private val DATE_MEDIUM:Int = model("date.medium").toInt
        
  private val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  
  /**
   * Represent transactions as a time ordered sequence of Markov States;
   * the result is directly used to build the respective Markov Model
   */
  def buildStateSeq(transactions:RDD[CommerceTransaction]):RDD[StateSequence] = {

    /*
     * First compute state parts (amount & ticket) on a per transaction
     * basis; then group results with respect to user and filter those
     * datasets that have more than one transaction
     */
    val prestates = transactions.map(t => {
      
      val (site,user,order,timestamp,items) = (t.site,t.user,t.order,t.timestamp,t.items)
      
      val astate = stateByAmount(items)
      val tstate = stateByPrice(items)
      
      (site,user,order,timestamp,(astate + tstate))
      
    }).groupBy(_._2).filter(valu => valu._2.size >= 2)
  
    /*
     * Build a time ordered sequence of Markov states from all
     * transactions (states) on a per user basis
     */
    prestates.map(valu => {
      
      /* Sort transaction data by timestamp */
      val data = valu._2.toList.sortBy(_._4)
      
      /**
       * Evaluate records, determine missing date state by classifying
       * the time elapsed by between subsequent transactions, and finally
       * build a state sequence
       */
      val (site,user,order,starttime,pstates) = data.head
      
      var endtime = starttime
      val states = ArrayBuffer.empty[String]

      for (rec <- data.tail) {
        
        val state = rec._5 + stateByDate(rec._4,endtime)
        states += state
        
        endtime = rec._4
        
      }
      
      new StateSequence(site,user,states.toList)
    
    })
    
  }
  
  /**
   * Determine the amount spent by a transaction and assign a classifier, "H", "N", "L"; 
   * this classifier specifies the (1) part of a transaction state description
   */
  private def stateByAmount(items:List[CommerceItem]):String = {
    
    val amount = items.map(item => item.price).sum
    (if (amount > AMOUNT_HIGH) "H" else if (amount > AMOUNT_NORM) "N" else "L")
    
  }
  
  /**
   * Determine whether transaction includes high price ticket item and assign a classifier "H", "N";
   * this classifier specifies the (2) part of a transaction state description. Actually, we do not
   * distinguish between transactions with one or more high price items
   */
  
  private def stateByPrice(items:List[CommerceItem]):String = {
    
    val states = items.map(item => {
      if (item.price > PRICE_HIGH) "H" else "N"      
    })
    
    if (states.contains("H")) "H" else "N"
   
  }
 
  /**
   * Time elapsed since last transaction
   * 
   * S : small
   * M : medium
   * L : large
   * 
   */
  private def stateByDate(ndate:Long,pdate:Long):String = {
    
    val d = (ndate -pdate) / DAY
    val dstate = (
        if (d < DATE_SMALL) "S"
        else if (d < DATE_MEDIUM) "M"
        else "L")
    
    dstate
  
  }
}