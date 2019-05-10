import org.apache.spark.graphx.{Graph => Graph1, VertexId, Edge}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Graph {
    def main ( args: Array[String] ) : Unit = {
    val conf = new SparkConf().setAppName("ConnGraph")
    val sc = new SparkContext(conf)
												  
	var inp_graph = sc.textFile(args(0)).map( line => { val ip = line.split(",")
                                                      	(ip(0).toLong,
                                                      	ip(0).toLong,
                                                      	ip.drop(1).toList.map(_.toLong))
                                                      })  

	var edges =  inp_graph.flatMap(map => map match{ case (vid, g, adj) => adj.map(m => Edge(vid,m,g))})

	var graph = Graph1.fromEdges(edges, "defaultProperty").mapVertices((id, _)=>id)
	
	val num : Int = 5
	var ccgraph = graph.pregel(Long.MaxValue,5)(
        (id,grp,newGrp)=> math.min(grp,newGrp),
        triplet=>{
            if(triplet.attr<triplet.dstAttr){
              Iterator((triplet.dstId,triplet.attr))
            }else if((triplet.srcAttr<triplet.attr)){
              Iterator((triplet.dstId,triplet.srcAttr))
            }else{
              Iterator.empty
            }
        },
        (a,b)=>math.min(a,b)
	)

	ccgraph.vertices.map(v => (v._2, 1))
				.reduceByKey((m, n) => (m+n))
				.sortByKey(true, 1)
				.collect()
				.foreach(println)
	
	
	}
}