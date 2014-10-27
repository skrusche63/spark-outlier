package de.kp.spark.outlier.io
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
import org.elasticsearch.common.xcontent.XContentBuilder
  
object ElasticBuilderFactory {
  /*
   * Definition of common parameters for all indexing tasks
   */
  val SITE_FIELD:String = "site"
  val TIMESTAMP_FIELD:String = "timestamp"

  /******************************************************************
   *                          ITEM
   *****************************************************************/

  val USER_FIELD:String = "user"

  val GROUP_FIELD:String = "group"
  val ITEM_FIELD:String  = "item"

  val PRICE_FIELD:String  = "price"

  def getBuilder(builder:String,mapping:String,names:List[String]=List.empty[String],types:List[String]=List.empty[String]):XContentBuilder = {
    
    builder match {

      case "item" => new ElasticItemBuilder().createBuilder(mapping)

      case "feature" => new ElasticFeatureBuilder().createBuilder(mapping,names,types)
      
      case _ => null
      
    }
  
  }

}