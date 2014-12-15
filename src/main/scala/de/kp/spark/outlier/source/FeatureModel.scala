package de.kp.spark.outlier.source
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._

import de.kp.spark.outlier.model.LabeledPoint
import de.kp.spark.outlier.spec.Features

import scala.collection.mutable.ArrayBuffer

class FeatureModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[LabeledPoint] = {

    val spec = sc.broadcast(Features.get(req))
    rawset.map(data => {
      
      val fields = spec.value

      val label = data(fields.head)
      val features = ArrayBuffer.empty[Double]
      
      for (field <- fields.tail) {
        features += data(field).toDouble
      }
      
      new LabeledPoint(label,features.toArray)
      
    })

  }
  
  def buildFile(req:ServiceRequest,rawset:RDD[String]):RDD[LabeledPoint] = {
    
    rawset.map(valu => {
      
      val Array(label,features) = valu.split(",")  
      new LabeledPoint(label,features.split(" ").map(_.toDouble))
    
    })
    
  }

  def buildJDBC(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[LabeledPoint] = {

    val spec = sc.broadcast(Features.get(req))
    rawset.map(data => {
      
      val fields = spec.value

      val label = data(fields.head).asInstanceOf[String]
      val features = ArrayBuffer.empty[Double]
      
      for (field <- fields.tail) {
        features += data(field).asInstanceOf[Double]
      }
      
      new LabeledPoint(label,features.toArray)
      
    })
    
  }

  def buildParquet(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[LabeledPoint] = {

    val spec = sc.broadcast(Features.get(req))
    rawset.map(data => {
      
      val fields = spec.value

      val label = data(fields.head).asInstanceOf[String]
      val features = ArrayBuffer.empty[Double]
      
      for (field <- fields.tail) {
        features += data(field).asInstanceOf[Double]
      }
      
      new LabeledPoint(label,features.toArray)
      
    })
    
  }

}