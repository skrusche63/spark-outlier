package de.kp.spark.outlier.spec
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

import de.kp.spark.outlier.redis.RedisCache

import scala.xml._
import scala.collection.mutable.ArrayBuffer

object Features extends Serializable {
  
  val path = "features.xml"

  def get(uid:String):List[String] = {

    val fields = ArrayBuffer.empty[String]
  
    try {
          
      if (RedisCache.fieldsExist(uid)) {     
        
        val fieldspec = RedisCache.fields(uid)
        for (field <- fieldspec.items) {
          fields += field.name
        }
    
      } else {
        
        val root = XML.load(getClass.getClassLoader.getResource(path))  
        for (field <- root \ "field") {
          fields += field.text 
        }
      
     }
   
      
    } catch {
      case e:Exception => {}
    }
    
    fields.toList
    
  }

}