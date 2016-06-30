package com.deepera.ctg

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import Array._
import scala.collection.mutable._

object RawShortestPath{

	def parsePoint(lines: Array[String]): Array[String] = {
    	val points = (lines.map(_.split(',')(0)))
    	points
  	}

  	def parseEdge(lines:Array[String]): Array[Array[String]] = {
  		val edges = lines.map(_.split(','))
  		edges
  	}

	def main(args: Array[String]){
		val conf = new SparkConf()
		val sc : SparkContext = new SparkContext(conf.setAppName("ShortestPath"))

		//construct edges from raw data
		val triplets = sc.textFile(args(0)).flatMap { line =>
		  	if (!line.isEmpty && line(0) != '#') {
		    	val lineArray = line.split(",")
		    	if (lineArray.length < 5) {
		      		None
		    	} else {
		    		if(line(1) == "01"){
		      			val t = new EdgeTriplet[String, String]
		      			t.srcId = lineArray(0).hashCode
		      			t.srcAttr = lineArray(0)
		      			t.dstId = lineArray(2).hashCode
		      			t.dstAttr = lineArray(2)
		      			t.attr = lineArray(5)
		      			Some(t)
		    		}else{
		    			val t = new EdgeTriplet[String, String]
		      			t.srcId = lineArray(2).hashCode
		      			t.srcAttr = lineArray(2)
		      			t.dstId = lineArray(0).hashCode
		      			t.dstAttr = lineArray(0)
		      			t.attr = lineArray(5)
		      			Some(t)
		    		}
		    	}
		  	} else {
		    	None
		  	}
		}
		//extract vertices & edges
		val vertices = triplets.flatMap(t => Array((t.srcId, t.srcAttr), (t.dstId, t.dstAttr)))
		val edges = triplets.map(t => t: Edge[String])
		//build a graph
		val graph = Graph(vertices, edges)

		//applications
		println(graph.vertices.filter { case (id, (num)) => num == "931B6F66B9C58D645A12978A798C1B4D" }.count)
		// Count all the edges where src > dst
		println(graph.edges.filter(e => e.srcId > e.dstId).count)
		println("Number of vertices: %d", graph.numVertices)
		println("Number of edges: %d", graph.numEdges)

		//val landmarks = triplets.flatMap(t =>Seq(t.srcId, t.dstId))
		val landmarks = triplets.flatMap(t =>Seq("931B6F66B9C58D645A12978A798C1B4D".hashCode))
		val result = ShortestPaths.run(graph, Seq("931B6F66B9C58D645A12978A798C1B4D".hashCode))

		val SPs = result.vertices
		//println(SPs.collect)
		
		for (e <- SPs.collect){
			if (e._2.contains("931B6F66B9C58D645A12978A798C1B4D".hashCode)){
				println((e._1, e._2.map{ case (k,v) => v}))
			}
		}
		println("DD6B46026BDC6996216F6F6B63D11379".hashCode)
	}
}