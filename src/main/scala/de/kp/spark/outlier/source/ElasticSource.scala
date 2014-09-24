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

import org.apache.hadoop.io.{ArrayWritable,MapWritable,NullWritable,Text}

import org.elasticsearch.hadoop.mr.EsInputFormat

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import de.kp.spark.outlier.Configuration

import de.kp.spark.outlier.model.LabeledPoint
import de.kp.spark.outlier.spec.{DetectorSpec,PredictorSpec}

import scala.collection.mutable.ArrayBuffer

// TODO: Check whether a single conf for features and items is sufficient

class ElasticSource(@transient sc:SparkContext) extends Serializable {
          
  /* Retrieve data from Elasticsearch */    
  val conf = Configuration.elastic                          
 
  /**
   * Load labeled features from an Elasticsearch cluster
   */
  def features():RDD[LabeledPoint] = {
    
    val spec = sc.broadcast(DetectorSpec.get)
    
    /* Connect to Elasticsearch */
    val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val dataset = source.map(hit => toMap(hit._2))

    dataset.map(data => {
      
      val fields = spec.value

      val label = data(fields.head)
      val features = ArrayBuffer.empty[Double]
      
      for (field <- fields.tail) {
        features += data(field).toDouble
      }
      
      new LabeledPoint(label,features.toArray)
      
    })
    
  }
  /**
   * Load ecommerce items that refer to a certain site (tenant), user
   * and transaction or order
   */
  def items():RDD[(String,String,String,Long,String,Float)] = {
    
    val spec = sc.broadcast(PredictorSpec.get)
    
    /* Connect to Elasticsearch */
    val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    val dataset = source.map(hit => toMap(hit._2))

    dataset.map(data => {
      
      val site = data(spec.value("site")._1)
      val timestamp = data(spec.value("timestamp")._1).toLong

      val user = data(spec.value("user")._1)      
      val order = data(spec.value("order")._1)

      val item  = data(spec.value("item")._1)
      val price  = data(spec.value("price")._1).toFloat
      
      
      (site,user,order,timestamp,item,price)
      
    })
    
  }
  
  /**
   * A helper method to convert a MapWritable into a Map
   */
  private def toMap(mw:MapWritable):Map[String,String] = {
      
    val m = mw.map(e => {
        
      val k = e._1.toString        
      val v = (if (e._2.isInstanceOf[Text]) e._2.toString()
        else if (e._2.isInstanceOf[ArrayWritable]) {
        
          val array = e._2.asInstanceOf[ArrayWritable].get()
          array.map(item => {
            
            (if (item.isInstanceOf[NullWritable]) "" else item.asInstanceOf[Text].toString)}).mkString(",")
            
        }
        else "")
        
    
      k -> v
        
    })
      
    m.toMap
    
  }

}