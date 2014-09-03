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

object SequenceMetric {
  
  /* 
   * Miss Probability Metric
   * 
   * For any pair of consecutive transaction states t(i) and t(j) in a sequence, 
   * the following quantity is calculated: For the row corresponding to t(i), we 
   * are summing all the probabilities except for the target state t(j).
   * 
   * F(t(i), t(j)) = Sum(P(t(i), t(k)) | k != j) where P(t(i), t(k)) is the probability 
   * of transitioning from transaction state t(i) to t(k)
   * 
   * Then we sum F over all the transaction state pairs in the sequence and normalize by 
   * the number of such pairs.
   */
  
  def missProbMetric(states:List[String],model:TransitionMatrix):Double = {
    
    var F:Double = 0
    var count:Int = 0
    
    val STATE_DEFS = StateModel.FD_STATE_DEFS
    
    for (i <- 1 until states.size) {
      
      val srcIndex = STATE_DEFS.indexOf(states(i-1))
      val tarIndex = STATE_DEFS.indexOf(states(i))

      /* Sum all probabilities except the target state */
	  for (j <- 0 until STATE_DEFS.length) {
		if (j != tarIndex)
		  F += model.get(srcIndex,j)
	  }

      count += 1
    }
    
    val metric = F / count
    metric
    
  }

  /*
   * Miss Rate Metric 
   * 
   * For any transition, if transition corresponds to the maximum probability target state, the value is 0, otherwise it’s 1. 
   * 
   * F(t(i), t(j)) = 0 if t(j) = t(k) else 1 where t(k) is the target state when P(t(i), t(k)) = max(P(t(i), t(l)) for all l
   * 
   * Then we sum F over all the transaction state pairs in the sequence and normalize by 
   * the number of such pairs.
   */
  def missRateMetric(states:List[String],model:TransitionMatrix):Double = {
     
    var F:Double  = 0
    var count:Int = 0
    
    val STATE_DEFS = StateModel.FD_STATE_DEFS
    
    for (i <- 1 until states.size) {
      
      val srcIndex = STATE_DEFS.indexOf(states(i-1))
      val tarIndex = STATE_DEFS.indexOf(states(i))

      val maxIndex = STATE_DEFS.indexOf(model.getRow(srcIndex).max)
      
      F = (if (tarIndex == maxIndex) 0 else 1)
	  count += 1

    }
    
    val metric = F / count
    metric    

  }
	
  /*
   * Entropy Reduction Metric
   * 
   * We calculate two quantities F and G as below. For a given row, F is the entropy excluding target state for the state pair 
   * under consideration. G is the entropy for the whole row.
   * 
   * F(t(i), t(j)) = sum (-P(t(i), t(k)) log(P(t(i), t(k)) | t(k) != t(j)
   * G(t(i)) = sum (-P(t(i), t(k)) log(P(t(i), t(k))
   * 
   * We sum F and G over all consecutive state pairs and divide the two sums.
   */
  def entropyReductionMetric(states:List[String],model:TransitionMatrix):Double = {

    var F:Double = 0
    var G:Double = 0
    
    val STATE_DEFS = StateModel.FD_STATE_DEFS
    
    for (i <- 1 until states.size) {
      
      val srcIndex = STATE_DEFS.indexOf(states(i-1))
      val tarIndex = STATE_DEFS.indexOf(states(i))

      for (j <- 0 until STATE_DEFS.length) {
        
        val prob = model.get(srcIndex,j)
        val entropy = -prob * Math.log(prob)
        
        
        if (j != tarIndex) F += entropy
        G += entropy

      }

    }
    
    val metric = F / G
    metric
    
  }
  
}