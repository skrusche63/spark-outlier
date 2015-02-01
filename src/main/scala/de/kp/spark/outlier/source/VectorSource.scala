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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.source._
import de.kp.spark.outlier.Configuration

import de.kp.spark.outlier.model._
import de.kp.spark.outlier.model.LabeledPoint

import de.kp.spark.outlier.spec.Features

/**
 * VectorSource is an abstraction layer on top the physical 
 * data sources supported by KMeans outlier detection
 */
class VectorSource(@transient sc:SparkContext) {

  private val model = new VectorModel(sc)
  private val config = Configuration
  
  def get(req:ServiceRequest):RDD[LabeledPoint] = {
   
    val source = req.data(Names.REQ_SOURCE)
    source match {
      
      /* 
       * Discover clusters from feature set persisted as an appropriate search 
       * index from Elasticsearch; the configuration parameters are retrieved 
       * from the service configuration 
       */    
      case Sources.ELASTIC => {
        
        val rawset = new ElasticSource(sc).connect(config,req)
        model.buildElastic(req,rawset)
        
      }
      /* 
       * Discover clusters from feature set persisted as a file on the (HDFS) 
       * file system; the configuration parameters are retrieved from the service 
       * configuration  
       */    
      case Sources.FILE => {
       
        val store = req.data(Names.REQ_URL)
         
        val rawset = new FileSource(sc).connect(store,req)        
        model.buildFile(req,rawset)
        
      }
      /*
       * Discover clusters from feature set persisted as an appropriate table 
       * from a JDBC database; the configuration parameters are retrieved from 
       * the service configuration
       */
      case Sources.JDBC => {
    
        val fields = Features.get(req).map(kv => kv._2).toList  
        
        val rawset = new JdbcSource(sc).connect(config,req,fields)
        model.buildJDBC(req,rawset)
        
      }
      /*
       * Discover clusters from feature set persisted as a parquet file on HDFS; 
       * the configuration parameters are retrieved from the service configuration
       */
      case Sources.PARQUET => {
       
        val store = req.data(Names.REQ_URL)
        
        val rawset = new ParquetSource(sc).connect(store,req)
        model.buildParquet(req,rawset)
        
      }
      
      case _ => null
      
    }

  }
}