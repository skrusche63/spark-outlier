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

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import de.kp.spark.core.model.LabeledPoint

object Optimizer {

  /**
   * Determine from a range of cluster numbers that number where the mean
   * entropy of all cluster labels is minimal; note, that the entropy is 
   * an indicator for the homogenity of the cluster labels
   */ 
  def optimizeByEntropy(data:RDD[LabeledPoint],range:Range,iterations:Int):Int = {
   
    val scores = range.par.map(k => (k, clusterEntropy(data,k,iterations))).toList
    scores.sortBy(_._2).head._1
  
  }
  
  def clusterEntropy(data: RDD[LabeledPoint],clusters:Int,iterations:Int):Double = {

    val vectors = data.map(point => Vectors.dense(point.features))
    val model = KMeans.train(vectors,clusters,iterations)

    val entropies = data.map(point => {
      
      val cluster = model.predict(Vectors.dense(point.features))
      (cluster,point.label)
      
    }).groupBy(_._1).map(data => MathHelper.strEntropy(data._2.map(_._2))).collect()

    entropies.sum / entropies.size
    
  }

  /**
   * Determine from a range of cluster numbers that number where the mean
   * distance between cluster points and their cluster centers is minimal
   */ 
  def optimizeByDistance(data:RDD[LabeledPoint],range:Range,iterations:Int):Int = {

    val scores = range.par.map(k => (k, clusterDistance(data, k, iterations))).toList
    scores.sortBy(_._2).head._1
    
  }

  def distance(a:Array[Double], b:Array[Double]) = 
    Math.sqrt(a.zip(b).map(p => p._1 - p._2).map(d => d * d).sum)

  /**
   * This method calculates the mean distance of all data (vectors) from 
   * their centroids, given certain clustering parameters; the method may
   * be used to score clusters
   */
  def clusterDistance(data: RDD[LabeledPoint], clusters:Int, iterations:Int):Double = {
    
    val vectors = data.map(point => Vectors.dense(point.features))
    val model = KMeans.train(vectors,clusters,iterations)
    /**
     * Centroid: Vector that specifies the centre of a certain cluster
     */
    val centroids = model.clusterCenters
  
    val distances = data.map(point => {
      
      val cluster = model.predict(Vectors.dense(point.features))
      val centroid = centroids(cluster)
      
      distance(centroid.toArray,point.features)
      
    }).collect()
    
    distances.sum / distances.size

  }

}