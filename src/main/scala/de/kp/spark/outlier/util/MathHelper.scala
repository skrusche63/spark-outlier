package de.kp.spark.outlier.util
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

object MathHelper {

  /**
   * Entropy of a dataset containing integers
   */
  def intEntropy(data:TraversableOnce[Int]):Double = {

    val invLog2 = 1.0 / Math.log(2)

    val positives = data.filter(_ > 0)
    if (positives.size > 0) {
      
      val sum: Double = positives.sum
      val invSum = 1.0 / sum.toDouble
      
      positives.map {positive =>
    
        val p = positive.toDouble * invSum
        -p * Math.log(p)
     
      }.sum
    
    } else {
      0.0
    }
    
  }
  
  /**
   * Entroy of a dataset containing strings; it may be
   * used as a measure of the homogenity of the strings
   */
  def strEntropy(data:TraversableOnce[String]):Double = {
    
    val invLog2 = 1.0 / Math.log(2)

    val len = data.size
    if (len > 1) {
      
      val invLen = 1.0 / len.toDouble
      var ent = 0.0

      for (str <- data.toList.distinct) {
        /*
         * Probability to find a certain value within the dataset
         */
        val pstr = data.count(x => x == str).toDouble * invLen
        ent -= pstr * Math.log(pstr) * invLog2
        
      }
      
      ent

    } else {
      0.0
    
    }
  
  }
  
  /** 
   * Data is a distributed list of feature vectors (Array[Double]) with the
   * following semantic: vector = [f_0,f_1,f_2, ...]; i.e. each vectors holds
   * a certain value for feature i at position i. Normalizing those data means
   * that one has to normalize all values of feature f_0, all values of f_1 etc  
   */
  def normalize(data:RDD[Array[Double]]):RDD[Array[Double]] = {
    
    val total = data.count()

    /*
     * Each column of the data matrix is assigned to a certain feature;
     * we therefore have to sum up the values of each column independently
     * and build the mean value
     */
    val sums = data.reduce((a,b) => a.zip(b).map(t => t._1 + t._2))
    val means = sums.map(_ / total)

    /*
     * We build the standard deviation for the values of each column
     */
    val len = sums.length
    
    val init = new Array[Double](len)
    val sumSquares = data.fold(init)((a,b) => a.zip(b).map(t => t._1 + t._2*t._2))

    val stdevs = sumSquares.zip(sums).map {
      case(sumSq,sum) => Math.sqrt(total*sumSq - sum*sum) / total 
    }

    /*
     * Finally for each column (or feature), each single values gets
     * normalized using the mean value and standard deviations 
     */
    val normdata = data.map(
        
      (_,means,stdevs).zipped.map((value,mean,stdev) => {
        if (stdev <= 0) (value-mean) else (value-mean) / stdev
       
      })
    
    )
      
    normdata
    
  }

}