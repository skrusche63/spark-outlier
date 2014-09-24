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
import de.kp.spark.outlier.model.LabeledPoint

/**
 * FeatureSource is an abstraction layer on top the physical 
 * data sources supported by KMeans outlier detection
 */
class FeatureSource(@transient sc:SparkContext) {

  def get(source:String):RDD[LabeledPoint] = {

    source match {
      
      /* 
       * Discover outliers from feature set persisted as an appropriate search 
       * index from Elasticsearch; the configuration parameters are retrieved 
       * from the service configuration 
       */    
      case Sources.ELASTIC => new ElasticSource(sc).features
      /* 
       * Discover outliers from feature set persisted as a file on the (HDFS) 
       * file system; the configuration parameters are retrieved from the service 
       * configuration  
       */    
      case Sources.FILE => new FileSource(sc).features
      /*
       * Discover outliers from feature set persisted as an appropriate table 
       * from a JDBC database; the configuration parameters are retrieved from 
       * the service configuration
       */
      //case Sources.JDBC => new JdbcSource(sc).connect(req.data)
      /*
       * Discover outliers from feature set persisted as an appropriate table 
       * from a Piwik database; the configuration parameters are retrieved from 
       * the service configuration
       */
      //case Sources.PIWIK => new PiwikSource(sc).connect(req.data)
      
      case _ => null
      
    }

  }
}