package gebd.count;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import scala.Tuple2;


public class count {
public static void main(String[] args) {
	
	Logger.getLogger("org").setLevel(Level.ERROR);
	Logger.getLogger("akka").setLevel(Level.ERROR);
	
	SparkConf sc = new SparkConf();

	sc.setAppName("Triangle Counting");
	sc.setMaster("local[*]");
	
	JavaSparkContext jsc = new JavaSparkContext(sc);
	
	
	JavaRDD<String> rdd = jsc.textFile("data/dataset.txt");
	
	/* ________________________________________________________________________*/
	
	//	PRE- PROCESSING 
	
	//	Estraiamo gli archi in tuple <n1, n2>
	JavaPairRDD<Integer, Integer> archi = rdd.mapToPair(x-> new Tuple2<Integer, Integer>(Integer.parseInt(x.split("	")[0]),
			Integer.parseInt(x.split("	")[1])));
	System.out.println("Archi: "+ archi.collect());

	//[0,1], [0,2]--> [0,{1,2}]
	//<n1,n2>, <n1,n3> --> <n1,{n2,n3}>
	JavaPairRDD<Integer, Iterable<Integer>> group =archi.groupByKey();
	//System.out.println(group.collect());
	
	
	// <0,{1, 2, 3, 4, 5}> --> <0,{1,5}>,<0,{2,5}>, ...
	// <n1,{n2,n3,n4..}> --> <n1,<n2,grado(n1)>>,<n1,<n3,grado(n1)>>
	JavaPairRDD<Integer, Tuple2<Integer,Integer>> degree_n1 = group.flatMapToPair(t->{
		ArrayList<Tuple2<Integer,Tuple2<Integer,Integer>>> output=new ArrayList<Tuple2<Integer,Tuple2<Integer,Integer>>>();
        int counter=0;
        for(Integer s:t._2){
            counter++;
        }
        for(Integer s:t._2){
            output.add(new Tuple2<Integer,Tuple2<Integer,Integer>>(t._1,new Tuple2<Integer, Integer>(s,counter)));
        }
        return output.iterator();
    });
	System.out.println("Degree primo nodo: "+ degree_n1.collect());
	
	
	// <0,{1,5}>, <0,{2,5}> --> <1,{0,5}>, <2,{0,5}>, ...
	// <n1,<n2,grado(n1)>> --> <n2,<n1,grado(n1)>>
	JavaPairRDD<Integer, Tuple2<Integer,Integer>> turn = degree_n1.mapToPair(l->new Tuple2<Integer, Tuple2<Integer,Integer>>(l._2._1 , new Tuple2(l._1 , l._2._2)));
	System.out.println("Turn: "+turn.collect());
	
	
	//[1,{0,5}], [2,{0,5}] -> (0,[(4,1), (2,4), (1,2), (3,2), (5,2)]) ...
	// <n2,<n1,grado(n1)>> --> <n2,<<n1,grado(n1)>,freq>
	JavaPairRDD<Integer, Iterable<Tuple2<Integer,Integer>>> freq= turn.groupByKey();
	// System.out.println(freq.collect());
	// System.out.println(freq.count());
	
	//(0,[(4,1), (2,4), (1,2), (3,2), (5,2)]) -> (0,[(4,1), grado(0)]), (0,[(2,4), grado(0)]), ...  
	//<n2,<<n1,grado(n1)>,freq> --> <n2,<<n1,grado(n1)>,grado(n2)>>
	JavaPairRDD<Integer, Tuple2<Tuple2<Integer,Integer>,Integer>> degree_n2 = freq.flatMapToPair(t->{
		ArrayList<Tuple2<Integer,Tuple2<Tuple2<Integer,Integer>,Integer>>> output=new ArrayList<Tuple2<Integer,Tuple2<Tuple2<Integer,Integer>,Integer>>> ();
        int counter=0;
        for(Tuple2<Integer, Integer> s:t._2){
            counter++;
        }
        for(Tuple2<Integer, Integer> s:t._2){
            output.add(new Tuple2<Integer,Tuple2<Tuple2<Integer,Integer>,Integer>>(t._1,new Tuple2<Tuple2<Integer, Integer>,Integer>(s,counter)));
        }
        return output.iterator();
    });
	System.out.println("Degree secondo nodo: "+degree_n2.collect());
	
	// <n2,<<n1,grado(n1)}>,grado(n2)>> --> <<n2,grado(n2)>,<n1,grado(n1)>> , ...
	JavaPairRDD<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> coppie = degree_n2.mapToPair(x->{
	    Tuple2<Integer,Integer> t1 = new Tuple2<Integer,Integer>(x._2._1._1,x._2._1._2);
	    Tuple2<Integer,Integer> t2 = new Tuple2<Integer,Integer>(x._1,x._2._2);
	    return new Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> (t1,t2); 
	});
	System.out.println("Result pre-processing: "+ coppie.collect());
	//System.out.println(coppie.count());
	
	
	// ALGORITMO FFFk
	
	/*____________________________________________________________________________*/
	
	//  ROUND 1
	
	//	Sui nodi del grafo è possibile definire la relazione di ordine totale come segue:
	//  Dati 2 nodi (x e y), x<y se e solo se:
	// -> Il grado del nodo x è minore del grado del nodo y
	// -> I due nodi hanno lo stesso grado ma l'etichetta del nodo x è minore di quella del nodo y 
	//   ( assumendo che i nodi abbiano etichette univoche)

	// Filtriamo gli archi del grafo in modo tale che sia rispettata una delle precedenti condizioni
	
	//<<n1,grado(n1)>,<n2,grado(n2)>> con grado(n2)>grado(n1) oppure grado(n2)=grado(n1), n1<n2 
	JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> nodeDegrees1 = coppie.filter(x->x._1._2<x._2._2);
	JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> nodeDegrees2 = coppie.filter(x->(x._1._2.equals(x._2._2)) && (x._1._1<x._2._1));
	
	// PRIMO MAP
	// Archi <x,y> tali che x<y (y è il nodo preferito)
	
	JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> map1 = nodeDegrees1.union(nodeDegrees2);
	System.out.println("Map1:" + map1.collect());
	System.out.println(map1.count());
	
	// PRIMO REDUCE
	// utilizzando la trasformazione GroupByKey è possibile ottenere i nodi adiacenti al nodo x (in chiave)
	// il risultato sarà l'intorno superiore del nodo x: Gamma+(x)
			
	JavaPairRDD<Tuple2<Integer,Integer>, Iterable<Tuple2<Integer,Integer>>> reduce1 = map1.groupByKey().filter(l->size(l._2)>=2);
	System.out.println("Reduce1:" + reduce1.collect());	

	/*_____________________________________________________________________________*/
	
	//  ROUND 2
	
	// SECONDO MAP
	
	// INPUT 1 : <<n1,grado(n1)>,<n2,grado(n2)>> 
	// --> <<<n1,grado(n1)>,<n2,grado(n2)>>, "$"> se n2 è preferito ad n1 (condizione già verificata nel Map1)
	JavaPairRDD<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>,String> dollar = map1.mapToPair(x-> {
		return new Tuple2<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, String>(x,"$");
	});
	System.out.println("Map2.1:" + dollar.collect());
	
	// INPUT 2 : <u; {x1, x2, ... ,xn}> , i nodi 'x1, x2, ... ,xn' sono preferiti al nodo 'u' in chiave
	// --> <<x1,grado(x1)>,<x2,grado(x2)>> x2 preferito ad x1
	// --> <<x1,grado(x1)>,<x3,grado(x3)>> x3 preferito ad x1
	// --> <<x2,grado(x2)>,<x3,grado(x3)>> x3 preferito ad x2
	//.... tutte le possibili combinazioni a 2 a 2 degli elementi presenti in Iterable vanno in Chiave, nodo u in valore

	JavaPairRDD<Iterable<Tuple2<Integer,Integer>>, Tuple2<Integer,Integer>> nodoU = 
			reduce1.mapToPair(x->new Tuple2<Iterable<Tuple2<Integer,Integer>>, Tuple2<Integer,Integer>>(x._2, x._1));
	System.out.println("NodeU:" + nodoU.collect());
	
	JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>> coppieNodi = 
			nodoU.flatMapToPair(new coppieNodi ());
	System.out.println("Map2.2:" + coppieNodi.collect());
	
	
	// SECONDO REDUCE
	
	//	INPUT 2: raggruppando per chiave si ottiene una lista contenente in valore tutti i nodi
	//  associati allo stesso arco
	//	<(x1, x2); u1>, <(x1, x2); u2>  ----> <(x1, x2); {u1, u2, ... , un}>
	
	JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>> group2 = coppieNodi.groupByKey();
	System.out.println("Group archi:" + group2.collect());
	
	// INPUT 1: consideriamo solo le coppie che presentano il simbolo '$' in valore
	// il simbolo indica se (xi,xj) è effettivamente un arco, questo compare se è soddisfatta la condizione di preferenza 
	//	<(xi,xj),{u1,...uk}> ---> <(xi,xj),{u1,...,uk} U $}> 
	
	JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Iterable<Tuple2<Integer, Integer>>, String>> map2 =
			group2.join(dollar);
	System.out.println("Map2:" + map2.collect());
	
	// <(xi,xj),{u1,...,uk} U $}> ---> <(xi,xj); {u1, ... ,uk}>
	JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Iterable<Tuple2<Integer, Integer>>> reduce2
	        = map2.mapToPair
	        (x->new Tuple2<Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>,Iterable<Tuple2<Integer,Integer>>>(x._1,x._2._1));
	System.out.println("Reduce2:" + reduce2.collect());
	

	/*_____________________________________________________________________*/
	
    // ROUND 3
	
	// TERZO MAP
	
	// L'arco in chiave passa in valore
	//	<(x1,x2); {u1, ... ,uk}> ----> <{u1, ... ,uk}; (x1,x2)>
	JavaPairRDD<Iterable<Tuple2<Integer, Integer>>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer,Integer>>> turn2 = reduce2.mapToPair(l-> {
		Tuple2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> x = new Tuple2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>(l._1._1, l._1._2);
		return new Tuple2<Iterable<Tuple2<Integer, Integer>>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer,Integer>>> (l._2, x);
	});
	System.out.println("Nodo X:" + turn2.collect());
	
	// consideriamo i nodi presenti in chiave singolarmente 
	// <{u1, ... ,uk}; (x1,x2)> ----> <u1; (x1,x2)>, <u2; (x1,x2)> , ...,  <uk; (x1,x2)>
	JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>> flat = turn2.flatMapToPair(new iterableNodi ());
	System.out.println("Flat:" + flat.collect());
	
	// raggruppiamo per chiave
	//<u1; (x1,x2)>, <u2; (x1,x2)> , ...,  <uk; (x1,x2)> ----> <u1; {(x1,x2),(x3,x4),...}>
	JavaPairRDD<Tuple2<Integer, Integer>, Iterable<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>>> map3 = flat.groupByKey();
	System.out.println("Map3:" + map3.collect());
	
	
	// TERZO REDUCE 
	
	// Raggruppo tutti i nodi aventi u come chiave
	// In valore avrò il numero di archi collegati al nodo in chiave
	JavaPairRDD<Tuple2<Integer,Integer>,Integer> reduce3 = map3.mapToPair(l->new Tuple2(l._1,size(l._2)));
	System.out.println("Reduce3:" + reduce3.collect());
	
	// Sommando il numero di archi in tali sottografi si ottiene il numero di triangoli totali del grafo
	Integer triangleCounting = reduce3.map(l->l._2).reduce((x,y)->x+y);
	System.out.println("Numero di triangoli:" + triangleCounting);
	
	Scanner scan; 
	System.out.println("Premi invio per concludere l'esecuzione");
	scan = new Scanner(System.in);
	scan.next();
	
	// http://localhost:4040/jobs/
	
	/*_______________________________________________________________________*/


    // IMPLEMENTAZIONE DATABASE SU NEO4J
//
//    String uri = "bolt://localhost:7687";
//    AuthToken token = AuthTokens.basic("neo4j", "tg22");
//    Driver driver = GraphDatabase.driver(uri, token);
//    Session s = driver.session();
//    System.out.println("Connessione stabilita! ");
//
//    
//    
//    // CREAZIONE NODI IN NEO4J             	
//
//    List<Tuple2<Integer, Iterable<Tuple2<Integer,Integer>>>> Nodes = freq.collect();
//
//    for(Tuple2<Integer, Iterable<Tuple2<Integer,Integer>>> x : Nodes) {
//	    int n1 = x._1;
//	
//	    String query1 = "create (m {nodo: "+n1+"})";
//	    s.run(query1);
//	    }
//
//
//    
//    // CREAZIONE ARCHI IN NEO4J 
//    
//    List<Tuple2<Integer, Integer>> Edges = archi.collect();
//    for(Tuple2<Integer, Integer> x : Edges) {
//        int n1 = x._1;
//        int n2 = x._2;
//
//        String query2 = "match (n),(m) " + "where n.nodo = "+n1+" and m.nodo = "+n2+" "
//        + "create (n)-[a:nearby]->(m) ";
//        s.run(query2);
//        }
//    
	
   
    
}

	
    //Metodo per calcolare la lunghezza di una Lista (iterable)
    public static int size (Iterable<?> data) {
    	if (data instanceof Collection) {
    		return ((Collection<?>) data).size();
    		}
    	int counter = 0;
    	for (Object i : data) {
    		counter++;
    		}
    	return counter;
    	}


}


