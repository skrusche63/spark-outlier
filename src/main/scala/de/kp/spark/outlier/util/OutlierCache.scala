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

import de.kp.spark.outlier.Configuration
import de.kp.spark.outlier.model._

import java.util.Date

object OutlierCache {
  
  private val maxentries = Configuration.cache  
  private val cache = new LRUCache[(String,Long),List[(Double,LabeledPoint)]](maxentries)

  def add(uid:String,outliers:List[(Double,LabeledPoint)]) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = (uid,timestamp)
    val v = outliers
    
    cache.put(k,v)
    
  }
  
  def exists(uid:String):Boolean = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    (keys.size > 0)
    
  }
  
  def outliers(uid:String):List[(Double,LabeledPoint)] = {
    
    val keys = cache.keys().filter(key => key._1 == uid)
    if (keys.size == 0) {    
      null
      
    } else {
      
      val last = keys.sortBy(_._2).last
      cache.get(last) match {
        
        case None => null
        case Some(rules) => rules
      
      }
      
    }
  
  }

}