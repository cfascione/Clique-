package gebd.count;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;


public class coppieNodi implements PairFlatMapFunction<Tuple2<Iterable<Tuple2<Integer,Integer>>,Tuple2<Integer,Integer>>, 
Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>, Tuple2<Integer,Integer>> {

	public Iterator<Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>> call(
			Tuple2<Iterable<Tuple2<Integer,Integer>>, Tuple2<Integer,Integer>> x) throws Exception {
		

			ArrayList<Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>> lista = new ArrayList<Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>> ();
							
			for(Tuple2<Integer,Integer> i:x._1) {
				for(Tuple2<Integer,Integer> j:x._1) {
					if(i._1.compareTo(j._1)!=0) {
						if ((i._2<j._2)||(i._2.equals(j._2) && i._1<j._1)) {
							lista.add(new Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>>(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>(i,j), x._2));
						}	
					}
				}
			}
			return lista.iterator();
		
	}



}
