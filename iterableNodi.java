package gebd.count;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class iterableNodi implements PairFlatMapFunction<Tuple2<Iterable<Tuple2<Integer, Integer>>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer,Integer>>>,
Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> {

@Override
public Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>> call(
		Tuple2<Iterable<Tuple2<Integer, Integer>>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer,Integer>>> l)
		throws Exception {

	List<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>> output = 
			new ArrayList<Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>>();
	
	for (Tuple2<Integer,Integer> u : l._1) {
		output.add(new Tuple2<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer, Integer>>>(u, l._2));
	}
	
	return output.iterator();
	}

}
