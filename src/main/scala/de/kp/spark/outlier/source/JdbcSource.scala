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

import de.kp.spark.outlier.model._

import de.kp.spark.outlier.io.JdbcReader
import de.kp.spark.outlier.spec.{Features,Sequences}

import scala.collection.mutable.ArrayBuffer

class JdbcSource(@transient sc:SparkContext) extends Serializable {

  def features(params:Map[String,Any]):RDD[LabeledPoint] = {
    
    val uid = params("uid").asInstanceOf[String]         
    val fields = Features.get(uid)
    /*
     * Convert field specification into broadcast variable
     */
    val spec = sc.broadcast(fields)
    
    /* Retrieve site and query from params */
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]
    
    val rawset = new JdbcReader(sc,site,query).read(fields)
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
  
  def items(params:Map[String,Any]):RDD[(String,String,String,Long,String,Float)] = {
    
    val uid = params("uid").asInstanceOf[String]    
     
    val fieldspec = Sequences.get(uid)
    val fields = fieldspec.map(kv => kv._2._1).toList    
    /*
     * Convert field specification into broadcast variable
     */
    val spec = sc.broadcast(fieldspec)
    
    /* Retrieve site and query from params */
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]

    val rawset = new JdbcReader(sc,site,query).read(fields)
    rawset.map(data => {
      
      val site = data(spec.value("site")._1).asInstanceOf[String]
      val timestamp = data(spec.value("timestamp")._1).asInstanceOf[Long]

      val user = data(spec.value("user")._1).asInstanceOf[String] 
      val group = data(spec.value("group")._1).asInstanceOf[String]
      
      val item  = data(spec.value("item")._1).asInstanceOf[String]
      val price  = data(spec.value("price")._1).asInstanceOf[Float]
      
      (site,user,group,timestamp,item,price)
      
    })

  }

}