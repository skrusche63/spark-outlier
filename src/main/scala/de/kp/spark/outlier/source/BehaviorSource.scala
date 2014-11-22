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

import de.kp.spark.outlier.model.{Behavior,Sources}
import de.kp.spark.outlier.spec.Sequences

import scala.collection.mutable.ArrayBuffer

private case class Item(id:String,price:Float)

private case class Sequence(site:String,user:String,orders:List[(Long,List[Item])])

class BehaviorSource(@transient sc:SparkContext) {

  private val model = new BehaviorModel(sc)
  
  def get(data:Map[String,String]):RDD[Behavior] = {

    val uid = data("uid")
    
    val source = data("source")
    source match {
      /* 
       * Discover outliers from feature set persisted as an appropriate search 
       * index from Elasticsearch; the configuration parameters are retrieved 
       * from the service configuration 
       */    
      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(data)
        model.buildElastic(uid,rawset)
      
      }
      /* 
       * Discover outliers from feature set persisted as a file on the (HDFS) 
       * file system; the configuration parameters are retrieved from the service 
       * configuration  
       */    
       case Sources.FILE => {
         
         val path = Configuration.file()._1

         val rawset = new FileSource(sc).connect(data,path)
         model.buildFile(uid,rawset)
         
       }
       /*
        * Discover outliers from feature set persisted as an appropriate table 
        * from a JDBC database; the configuration parameters are retrieved from 
        * the service configuration
        */
       case Sources.JDBC => {
     
         val fields = Sequences.get(uid).map(kv => kv._2._1).toList    
         
         val rawset = new JdbcSource(sc).connect(data,fields)
         model.buildJDBC(uid,rawset)
         
       }

       case _ => null
      
    }
    
  }

}