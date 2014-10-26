package de.kp.spark.outlier.redis
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

import java.util.Date

import de.kp.spark.outlier.model._
import de.kp.spark.outlier.spec.FeatureSpec

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object RedisCache {

  val client  = RedisClient()
  val service = "outlier"

  def addFOutliers(uid:String, outliers:FOutliers) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "feature:" + service + ":" + uid
    val v = "" + timestamp + ":" + Serializer.serializeFOutliers(outliers)
    
    client.zadd(k,timestamp,v)
    
  }

  def addBOutliers(uid:String, outliers:BOutliers) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "behavior:" + service + ":" + uid
    val v = "" + timestamp + ":" + Serializer.serializeBOutliers(outliers)
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addStatus(uid:String, task:String, status:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "job:" + service + ":" + uid
    val v = "" + timestamp + ":" + Serializer.serializeJob(JobDesc(service,task,status))
    
    client.zadd(k,timestamp,v)
    
  }
  
  def behaviorExists(uid:String):Boolean = {

    val k = "behavior:" + service + ":" + uid
    client.exists(k)
    
  }
 
  def featuresExists(uid:String):Boolean = {

    val k = "feature:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def metaExists(uid:String):Boolean = {

    val k = "meta:" + uid
    client.exists(k)
    
  }
  
  def taskExists(uid:String):Boolean = {

    val k = "job:" + service + ":" + uid
    client.exists(k)
    
  }
  
  def features(uid:String):String = {

    val spec = FeatureSpec.get(uid)

    val k = "feature:" + service + ":" + uid
    val features = client.zrange(k, 0, -1)

    if (features.size() == 0) {
      Serializer.serializeFDetections(new FDetections(List.empty[FDetection]))
    
    } else {
      
      val last = features.toList.last
      val outliers = Serializer.deserializeFOutliers(last.split(":")(1)).items
      
      val detections = outliers.map(o => {
                  
        val (distance,point) = o
        val (label,values) = (point.label,point.features)
                  
        val features = ArrayBuffer.empty[FField]
        (1 until spec.length).foreach(i => {
                    
           val name  = spec(i)
           val value = values(i-1)
                    
           features += new FField(name,value)
                  
        })
                  
        new FDetection(distance,label,features.toList)
  
      }).toList
       
      Serializer.serializeFDetections(new FDetections(detections))
      
    }
  }
  
  def behavior(uid:String):String = {

    val k = "rule:" + service + ":" + uid
    val behavior = client.zrange(k, 0, -1)

    if (behavior.size() == 0) {
      Serializer.serializeBDetections(new BDetections(List.empty[BDetection]))
    
    } else {
      
      val last = behavior.toList.last
      val outliers = Serializer.deserializeBOutliers(last.split(":")(1)).items

      val detections = outliers.map(o => {
        new BDetection(o._1,o._2,o._3,o._4,o._5)
      }).toList
       
      Serializer.serializeBDetections(new BDetections(detections))

    }
  
  }
  
  def meta(uid:String):String = {

    val k = "meta:" + uid
    val metas = client.zrange(k, 0, -1)

    if (metas.size() == 0) {
      null
    
    } else {
      
      metas.toList.last
      
    }

  }
  
  /**
   * Get timestamp when job with 'uid' started
   */
  def starttime(uid:String):Long = {
    
    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      0
    
    } else {
      
      val first = jobs.iterator().next()
      first.split(":")(0).toLong
      
    }
     
  }
  
  def status(uid:String):String = {

    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      null
    
    } else {
      
      val job = Serializer.deserializeJob(jobs.toList.last)
      job.status
      
    }

  }

}