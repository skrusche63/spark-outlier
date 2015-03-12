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

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import de.kp.spark.core.model._
import de.kp.spark.outlier.util.{MathHelper,Optimizer}

/**
 * KMeansDetector is a general purpose outlier detector that 
 * detects outliers in sets of labeled features
 */
class KMeansDetector extends Serializable {
  
  def find(data:RDD[LabeledPoint],strategy:String="entropy",iterations:Int,top:Int):List[ClusteredPoint] = {
    
    val (k,normdata) = prepare(data,strategy,iterations)
    detect(normdata,k,iterations,top)
    
  }
  
  def detect(normdata:RDD[LabeledPoint],k:Int,iterations:Int,top:Int):List[ClusteredPoint] = {
    
    val sc = normdata.context
    
    /*
     * STEP #1: Compute KMeans model
     */   
    val vectors = normdata.map(point => Vectors.dense(point.features))

    val model = KMeans.train(vectors,k,iterations)
    val centroids = model.clusterCenters

    /*
     * STEP #2: Calculate the distances for all points from their clusters; 
     * outliers are those that have the farest distance
     */
    val bcmodel = sc.broadcast(model)
    val points = normdata.map(point => {
      
      val vector = Vectors.dense(point.features)
      
      val cluster = bcmodel.value.predict(vector)
      val centroid = bcmodel.value.clusterCenters(cluster)
      
      val distance = Optimizer.distance(centroid.toArray,vector.toArray)
 
      (cluster,distance,point)
      
    })
    
    /*
     * Retrieve top k features (LabeledPoint) with respect to their clusters;
     * the cluster identifier is used as a grouping mechanism to specify which
     * features belong to which centroid
     */
    val bctop = sc.broadcast(top)
    points.groupBy(_._1).flatMap(x => x._2.toList.sortBy(_._2).reverse.take(bctop.value)).map(data => {
    
      val (cluster,distance,point) = data
      new ClusteredPoint(cluster,distance,point)
      
    }).collect().toList
    
  }

  def prepare(data:RDD[LabeledPoint],strategy:String="entropy",iterations:Int):(Int,RDD[LabeledPoint]) = {
    
    /* 
     * STEP #1: Normalize data 
     */
    val idlabels = data.map(p => (p.id,p.label))
    
    val features = data.map(p => p.features)
    
    val normalized = MathHelper.normalize(features)    
    val normdata = idlabels.zip(normalized).map{case((id,label),features) => LabeledPoint(id,label, features)}
    
    /*
     * STEP #2: Find optimal number of clusters
     */
  
    /* Range of cluster center */
    val range = (5 to 40 by 5)
  
    val k = strategy match {
      
      case "distance" => Optimizer.optimizeByDistance(normdata, range, iterations)
        
      case "entropy"  => Optimizer.optimizeByEntropy(normdata, range, iterations)
      
    }
    
    (k, normdata)
  
  }
  
}