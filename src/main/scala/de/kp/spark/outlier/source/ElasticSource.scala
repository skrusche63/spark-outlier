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

import de.kp.spark.outlier.Configuration
import de.kp.spark.outlier.io.ElasticReader

import de.kp.spark.outlier.model.LabeledPoint
import de.kp.spark.outlier.spec.{Features,Sequences}

import scala.collection.mutable.ArrayBuffer

class ElasticSource(@transient sc:SparkContext) extends Serializable {
          
  /* Retrieve data from Elasticsearch */    
  val conf = Configuration.elastic                          
 
  def features(params:Map[String,Any]):RDD[LabeledPoint] = {
    
    val index = params("source.index").asInstanceOf[String]
    val mapping = params("source.type").asInstanceOf[String]
    
    val query = params("query").asInstanceOf[String]
    
    val uid = params("uid").asInstanceOf[String]
    val spec = sc.broadcast(Features.get(uid))
    
    /* Connect to Elasticsearch */
    val rawset = new ElasticReader(sc,index,mapping,query).read
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

  def items(params:Map[String,Any]):RDD[(String,String,String,Long,String,Float)] = {
    
    val index = params("source.index").asInstanceOf[String]
    val mapping = params("source.type").asInstanceOf[String]
    
    val query = params("query").asInstanceOf[String]
    
    val uid = params("uid").asInstanceOf[String]
    val spec = sc.broadcast(Sequences.get(uid))
    
    /* Connect to Elasticsearch */
    val rawset = new ElasticReader(sc,index,mapping,query).read
    rawset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val group = data(spec.value("group")._1)

      val item  = data(spec.value("item")._1)
      val price  = data(spec.value("price")._1).toFloat
      
      (site,user,group,timestamp,item,price)
      
    })
    
  }

}