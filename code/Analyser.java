package cdistRest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.data.general.DefaultPieDataset;

import com.vdurmont.emoji.EmojiParser;

import scala.Tuple2;

public class Analyser {

	public static File saveTweets(String username, String filename1, String filename2)
			throws IOException, InterruptedException {
		TwitterCrawlerAsynchronous tc = new TwitterCrawlerAsynchronous(500, username);
		tc.requestTweets();
		List<Tweet> misTweets = tc.getTweets();
		File f2 = new File(filename1);

		synchronized (tc.wait_inner) {

			System.out.println("Guardando Tweets");
			File f1 = new File(filename2);
			PrintWriter pr1 = new PrintWriter(f1);
			PrintWriter pr2 = new PrintWriter(f2);

			misTweets.forEach(t -> {
				String noBreaksTweet = t.text.replace("\n", "").replace("\r", "");
				String noBreaksParsedEmojis = EmojiParser.parseToAliases(noBreaksTweet);
				String noBreaksNoEmojisTweet = EmojiParser.removeAllEmojis(noBreaksTweet);
				pr1.println(t.created_at + " ||| " + t.user.screen_name + " ||| " + noBreaksParsedEmojis);
				pr2.println(t.created_at + " ||| " + t.user.screen_name + " ||| " + noBreaksNoEmojisTweet);

			});

			System.out.println("Tweets procesados con emojis guardados en " + f1.getAbsolutePath());
			System.out.println("Tweets procesados sin emojis guardados en " + f2.getAbsolutePath());
			pr1.flush();
			pr2.flush();
			pr1.close();
			pr2.close();
		}

		tc.ShutDown();

		return f2;
	}

	public static void contarPalabras(String inputFile, String outputFile, JavaSparkContext sc) {

		JavaRDD<String> input = sc.textFile(inputFile);

		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});

		// Transform into word and count.
		// associate 1 per word
		// and then reduce by adding all the numbers per word (key)

		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2<String, Integer>(x, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile(outputFile);
	}

	public static void crearGrafica(double numVecesPersona[], String palabra) {
		System.out.println("Pablo Casado dice la palabra " + palabra + " " + numVecesPersona[0] + " veces");
		System.out.println("Pedro Sánchez dice la palabra " + palabra + " " + numVecesPersona[1] + " veces");
		System.out.println("Santiago Abascal dice la palabra " + palabra + " " + numVecesPersona[2] + " veces");
		System.out.println("Pablo Iglesias dice la palabra " + palabra + " " + numVecesPersona[3] + " veces");

		if (numVecesPersona[0] != 0 || numVecesPersona[1] != 0 || numVecesPersona[2] != 0 || numVecesPersona[3] != 0 ) {

			DefaultPieDataset tablaDatos = new DefaultPieDataset();

			tablaDatos.insertValue(0, "Pablo Casado: " + numVecesPersona[0], numVecesPersona[0]);
			tablaDatos.insertValue(1, "Pedro Sánchez: " + numVecesPersona[1], numVecesPersona[1]);
			tablaDatos.insertValue(2, "Santiago Abascal: " + numVecesPersona[2], numVecesPersona[2]);
			tablaDatos.insertValue(3, "Pablo Iglesias: " + numVecesPersona[3], numVecesPersona[3]);

			JFreeChart f = ChartFactory.createPieChart("Comparación políticos españoles - " + palabra, tablaDatos, true, false, false);
			ChartFrame freme = new ChartFrame("Ejemplo", f);
			freme.pack();
			freme.setVisible(true);
		}
	}
	
	public static double cantidadPalabra(String palabra, BufferedReader br) throws IOException {
		
		double cantidad = 0;
		String linea;
		while ((linea = br.readLine()) != null) {
			if (linea.contains(palabra)) {
				System.out.println(linea);
				cantidad = cantidad + Character.getNumericValue(linea.charAt(linea.length() - 2));
			}
		}
		return cantidad;
	}
	
	public static void main(String args[]) throws IOException, InterruptedException {
		
		String palabra;

		Scanner teclado = new Scanner(System.in);
		System.out.println("Escribe la palabra que quieras comparar");
		palabra = teclado.nextLine();
		
		File f1 = saveTweets("pablocasado_", "Tweets de Pablo Casado procesados", "Tweets de Pablo Casado sin emojis procesados");
		File f3 = saveTweets("sanchezcastejon", "Tweets de Pedro Sanchez procesados", "Tweets de Pedro Sanchez sin emojis procesados");
		File f5 = saveTweets("Santi_ABASCAL", "Tweets de Santiago Abascal procesados", "Tweets de Santiago Abascal sin emojis procesados");
		File f7 = saveTweets("PabloIglesias", "Tweets de Pablo Iglesias procesados", "Tweets de Pablo Iglesias sin emojis procesados");

		
		
		File f2 = new File("in.txt");
		f1.renameTo(f2);
		String inputFile = "/usr/lab/alum/0384129/Escritorio/eclipse/proyectoRESTspark/in.txt";
		String outputFile = "salida";
		
		File f4 = new File("in2.txt");
		f3.renameTo(f4);
		String inputFile2 = "/usr/lab/alum/0384129/Escritorio/eclipse/proyectoRESTspark/in2.txt";
		String outputFile2 = "salida2";
		
		File f6 = new File("in3.txt");
		f5.renameTo(f6);
		String inputFile3 = "/usr/lab/alum/0384129/Escritorio/eclipse/proyectoRESTspark/in3.txt";
		String outputFile3 = "salida3";
		
		File f8 = new File("in4.txt");
		f7.renameTo(f8);
		String inputFile4 = "/usr/lab/alum/0384129/Escritorio/eclipse/proyectoRESTspark/in4.txt";
		String outputFile4 = "salida4";
		
		JavaSparkContext sc = new JavaSparkContext("local", "wordcount", System.getenv("SPARK_HOME"), System.getenv("JARS"));

		contarPalabras(inputFile, outputFile, sc);
		contarPalabras(inputFile2, outputFile2, sc);
		contarPalabras(inputFile3, outputFile3, sc);
		contarPalabras(inputFile4, outputFile4, sc);
		
		
		
		File archivo = null;
		FileReader fr = null;
		BufferedReader br = null;

		File archivo2 = null;
		FileReader fr2 = null;
		BufferedReader br2 = null;
		
		File archivo3 = null;
		FileReader fr3 = null;
		BufferedReader br3 = null;

		File archivo4 = null;
		FileReader fr4 = null;
		BufferedReader br4 = null;

		try {
			// Apertura del fichero y creacion de BufferedReader para poder
			// hacer una lectura comoda (disponer del metodo readLine()).
			archivo = new File("salida/part-00000");
			fr = new FileReader(archivo);
			br = new BufferedReader(fr);

			archivo2 = new File("salida2/part-00000");
			fr2 = new FileReader(archivo2);
			br2 = new BufferedReader(fr2);
			
			archivo3 = new File("salida3/part-00000");
			fr3 = new FileReader(archivo3);
			br3 = new BufferedReader(fr3);

			archivo4 = new File("salida4/part-00000");
			fr4 = new FileReader(archivo4);
			br4 = new BufferedReader(fr4);
			
			double numVecesPersona[] = new double[4];
			
			numVecesPersona[0] = cantidadPalabra(palabra, br);
			numVecesPersona[1] = cantidadPalabra(palabra, br2);
			numVecesPersona[2] = cantidadPalabra(palabra, br3);
			numVecesPersona[3] = cantidadPalabra(palabra, br4);
			
			crearGrafica(numVecesPersona, palabra);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// En el finally cerramos el fichero, para asegurarnos
			// que se cierra tanto si todo va bien como si salta
			// una excepcion.
			try {
				if (null != fr) {
					fr.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

	}
}
