package de.kp.spark.outlier.actor
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

import de.kp.spark.core.model._
import de.kp.spark.core.io.ElasticWriter

import de.kp.spark.outlier.model._

import de.kp.spark.outlier.io.{ElasticBuilderFactory => EBF}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class OutlierTracker extends BaseActor {
  
  def receive = {
    
    case req:ServiceRequest => {
     
      val origin = sender    
      val uid = req.data("uid")

      req.task match {

        case "track:feature" => {
          
          val data = Map("uid" -> uid, "message" -> Messages.DATA_TO_TRACK_RECEIVED(uid))
          val response = new ServiceResponse(req.service,req.task,data,OutlierStatus.SUCCESS)	
      
          origin ! response
          
          createLabeledPoint(req)
          context.stop(self)
          
        }
        
        case "track:sequence" => {
          
          val data = Map("uid" -> uid, "message" -> Messages.DATA_TO_TRACK_RECEIVED(uid))
          val response = new ServiceResponse(req.service,req.task,data,OutlierStatus.SUCCESS)	
      
          origin ! response
          
          createSequence(req)
          context.stop(self)
          
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          origin ! failure(req,msg)
          context.stop(self)
          
        }
        
      }

    }
    
  }
  
  private def createLabeledPoint(req:ServiceRequest) {
          
    try {

      val index   = req.data("index")
      val mapping = req.data("type")
    
      val writer = new ElasticWriter()
    
      val readyToWrite = writer.open(index,mapping)
      if (readyToWrite == false) {
      
        writer.close()
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        /* Prepare data */
        val source = prepareLabeledPoint(req.data)
        /*
         * Writing this source to the respective index throws an
         * exception in case of an error; note, that the writer is
         * automatically closed 
         */
        writer.write(index, mapping, source)
        
      }
      
    } catch {
        
      case e:Exception => {
        log.error(e, e.getMessage())
      }
      
    } finally {

    }
    
  }
  
  private def prepareLabeledPoint(params:Map[String,String]):java.util.Map[String,Object] = {
    
    val now = new Date()
    val source = HashMap.empty[String,String]
    
    source += EBF.SITE_FIELD -> params(EBF.SITE_FIELD)
    source += EBF.TIMESTAMP_FIELD -> now.getTime().toString    
 
    /* 
     * Restrict parameters to those that are relevant to feature description;
     * note, that we use a flat JSON data structure for simplicity and distinguish
     * field semantics by different prefixes 
     */
    val records = params.filter(kv => kv._1.startsWith("lbl.") || kv._1.startsWith("fea."))
    for (rec <- records) {
      
      val (k,v) = rec
        
      val name = k.replace("lbl.","").replace("fea.","")
      source += k -> v      
      
    }

    source
    
  }
  
  private def prepareSequence(params:Map[String,String]):java.util.Map[String,Object] = {
    
    val source = HashMap.empty[String,String]
    
    source += EBF.SITE_FIELD -> params(EBF.SITE_FIELD)
    source += EBF.USER_FIELD -> params(EBF.USER_FIELD)
      
    source += EBF.TIMESTAMP_FIELD -> params(EBF.TIMESTAMP_FIELD)
 
    source += EBF.GROUP_FIELD -> params(EBF.GROUP_FIELD)
    source += EBF.ITEM_FIELD -> params(EBF.ITEM_FIELD)

    source += EBF.PRICE_FIELD -> params(EBF.PRICE_FIELD)

    source
    
  }

  private def createSequence(req:ServiceRequest) {
          
    try {

      val index   = req.data("index")
      val mapping = req.data("type")
    
      val writer = new ElasticWriter()
    
      val readyToWrite = writer.open(index,mapping)
      if (readyToWrite == false) {
      
        writer.close()
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        /* Prepare data */
        val source = prepareSequence(req.data)
        /*
         * Writing this source to the respective index throws an
         * exception in case of an error; note, that the writer is
         * automatically closed 
         */
        writer.write(index, mapping, source)
        
      }
      
    } catch {
        
      case e:Exception => {
        log.error(e, e.getMessage())
      }
      
    } finally {

    }
    
  }
 
}