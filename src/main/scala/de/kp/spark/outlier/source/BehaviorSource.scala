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

import de.kp.spark.outlier.model.{Behavior,Sources}

import scala.collection.mutable.ArrayBuffer

private case class Item(id:String,price:Float)

private case class Sequence(site:String,user:String,orders:List[(Long,List[Item])])

class BehaviorSource(@transient sc:SparkContext) {

  def get(data:Map[String,String]):RDD[Behavior] = {

    val source = data("source")
    val rawset = source match {
      /* 
       * Discover outliers from feature set persisted as an appropriate search 
       * index from Elasticsearch; the configuration parameters are retrieved 
       * from the service configuration 
       */    
      case Sources.ELASTIC => new ElasticSource(sc).items(data)
      /* 
       * Discover outliers from feature set persisted as a file on the (HDFS) 
       * file system; the configuration parameters are retrieved from the service 
       * configuration  
       */    
       case Sources.FILE => new FileSource(sc).items(data)
       /*
        * Discover outliers from feature set persisted as an appropriate table 
        * from a JDBC database; the configuration parameters are retrieved from 
        * the service configuration
        */
       case Sources.JDBC => new JdbcSource(sc).items(data)

       case _ => null
      
    }

    /*
     * Format: (site,user,order,timestamp,item,price)
     * 
     * Group source by 'site' & 'user' and aggregate all items of a 
     * single order, sort respective orders by time and publish as
     * user sequence
     */
    val sequences = rawset.groupBy(v => (v._1,v._2)).map(data => {

      val (site,user) = data._1
      /*
       * Aggregate all items of a certain order, and then sort these 
       * items by timestamp in ascending order.
       */
      val orders = data._2.groupBy(_._3).map(group => {

        val head = group._2.head

        val timestamp = head._4        
        val items = group._2.map(data => new Item(data._5,data._6)).toList

        (timestamp,items)
        
      }).toList.sortBy(_._1)
      
      new Sequence(site,user,orders)
      
    })

    new BehaviorModel().buildStates(sequences)
    
  }

}