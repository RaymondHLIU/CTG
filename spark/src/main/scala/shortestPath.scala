package com.deepera.ctg

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import Array._
import scala.collection.mutable._
import scala.io.Source

object ShortestPath{

	//load vertex and its long id 
	//return a hashmap
    def load_encoder(f: String, sc: SparkContext): HashMap[String, Long] = {
		//val vertices = sc.textFile(f).map{line =>  line.split(",")}
		val vertexMap = new HashMap[String, Long]()
		
		for (line <- Source.fromFile(f).getLines()){
			val e = line.split(",")
			if (e.size > 1){
				vertexMap.put(e(0).toString, e(1).toLong)
				println("vertexMap size: %d", vertexMap.size)
			}
		}
		
		println("vertexMap size: %d", vertexMap.size)
		println(vertexMap.getOrElse("11E1DC2ADE67786D3E85B8FFFF34B4D0", Long.MaxValue))
		vertexMap
	}

	//load vertex and its long id 
	//return a hashmap
	def load_decoder(f: String, sc: SparkContext): HashMap[Long, String] = {
		val vertexMap = new HashMap[String, Long]()
		
		for (line <- Source.fromFile(f).getLines()){
			val e = line.split(",")
			if (e.size > 1){
				vertexMap.put(e(0).toString, e(1).toLong)
			}
		}
		vertexMap
	}

	//translate vertex from its str id to long id
	//return Long.MaxValue if vertex not exist
	def encode_vertex(dict:HashMap[String, Long], vertex_id:String) = {
  		println(dict.size)
		println(dict("33547DF0B0F2FDE03CFF020AD079C4DD"))
		dict.getOrElse(vertex_id, Long.MaxValue)
	}

  	//translate vertex from its long id to its string id
  	//return 32 '0' if vertex not exist
  	def decode_vertex(dict:HashMap[Long, String], vertex_id:Long) = {
  		dict.getOrElse(vertex_id, "00000000000000000000000000000000")
  	}

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

		val edges = args(0)
		val vertices = args(1)

		// Load the edges as a graph
		val graph = GraphLoader.edgeListFile(sc, edges)
		val encoder = load_encoder(vertices, sc)
		val decoder = load_decoder(vertices, sc)

		//applications
		println("Number of vertices: %d", graph.numVertices)
		println("Number of edges: %d", graph.numEdges)
		println(graph.vertices.first())
		println(graph.vertices.filter { case (id, num) => id == 65722 }.count)

		//val landmarks = triplets.flatMap(t =>Seq(encode_vertex(encoder, "931B6F66B9C58D645A12978A798C1B4D")))
		val longID1 = encode_vertex(encoder, "B2447F8A991DDAEB5D3354251B1189D1")
		val longID2 = encode_vertex(encoder, "C6CA8F6E51B8A070300F693AA0FBB5CF")
		println("longID1: %d, %d", longID1, )
		println("longID2: %d", longID2)
		val landmarks = Seq(longID1, longID2)
		val result = ShortestPaths.run(graph, landmarks)
		val SPs = result.vertices



		for (e <- SPs.collect){
			for (l <- landmarks){
				if (e._2.contains(l)){
					println((e._1, e._2.map{ case (k,v) => v}))
				}
			}
		}

	}   
}