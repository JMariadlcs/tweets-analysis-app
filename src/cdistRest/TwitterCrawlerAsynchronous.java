package cdistRest;

import java.io.*;
import java.util.Scanner;
import javax.swing.JFileChooser;
import java.lang.*;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.xy.XYDataset;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.vdurmont.emoji.EmojiParser;

import kong.unirest.Callback;
import kong.unirest.GetRequest;
import kong.unirest.Headers;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import kong.unirest.UnirestException;
import scala.Tuple2;

public class TwitterCrawlerAsynchronous implements Callback<String> {

	private static final String bearer_token = "AAAAAAAAAAAAAAAAAAAAACm4IwEAAAAAuU1dgjUMv5Agbs%2Bh6itU0a4N19Q%3DDCJPeU5hBbFfjsj0JNGBVmqqMRFaAkSVRPONNeVhSgHokBa2Nm";
	private static final int MAX_TWEET_COUNT_PER_REQUEST = 200;

	private long max_id = Long.MAX_VALUE;
	private long tweetcount = 0;
	private String screen_name = "";
	private long pending = 0;
	long request_twetcount = 0;
	long tweets_obtained = 0;
	List<Tweet> tweet_list_total = new ArrayList<Tweet>();
	boolean finish = false;

	Object wait_inner = new Object();
	Object wait_outer = new Object();

	public TwitterCrawlerAsynchronous(int count, String screen_name) {
		this.tweetcount = count;
		this.screen_name = screen_name;
	}

	@Override
	public void completed(HttpResponse<String> response) {

		/* Control de tasa */
		Headers headers = response.getHeaders();
		String limit = headers.getFirst("x-rate-limit-limit");
		String remaining = headers.getFirst("x-rate-limit-remaining");
		String reset = headers.getFirst("x-rate-limit-reset");

		System.out.println("Tienes un límite total de " + limit);
		System.out.println("Puedes hacer " + remaining + " durante la ventana de 15 minutos");
		long now_milis = Instant.now().toEpochMilli();
		long now_seconds = now_milis / 1000;
		long reset_seconds = (Long.parseLong(reset) - now_seconds);
		System.out.println("La ventana se resetea en " + reset_seconds + " segundos");

		List<Tweet> tweet_list_request = Tweet.deserializeJsonArray(response.getBody());
		tweets_obtained = tweet_list_request.size();
		System.out.println("Recibidos " + tweets_obtained + " tweets");
		/* actualizamos tweetcount con los recibidos */
		pending -= tweets_obtained;

		/* actualizamos la lista total de tweets */
		tweet_list_total.addAll(tweet_list_request);

		/*
		 * PARADA si hemos hemos recibido en total de tweets, paramos
		 */
		if (pending <= 0)
			finish = true;

		/*
		 * PARADA si hemos hemos recibido menos de request_twetcount es que no hay más.
		 * Hay que finalizar
		 */

		if (tweets_obtained < request_twetcount)
			finish = true;

		if (!finish) {
			for (Tweet tw : tweet_list_request) {
				if (tw.id < max_id)
					max_id = tw.id;
			}
			nextRequest();
		} else {
			System.out.println("Hemos finalizado de pedir tweets");
			finish();
		}

	}

	@Override
	public void failed(UnirestException arg0) {
		System.out.println("The request has failed \n" + arg0.getMessage());
	}

	@Override
	public void cancelled() {
		System.out.println("The request has been canceled");

	}

	public void nextRequest() {
		long request_twetcount = 0;
		long tweets_obtained = 0;
		if (pending > MAX_TWEET_COUNT_PER_REQUEST) {
			request_twetcount = MAX_TWEET_COUNT_PER_REQUEST;
		} else {
			request_twetcount = pending;
		}

		CompletableFuture<HttpResponse<String>> json_str_Response = null;
		GetRequest getReq = null;
		getReq = Unirest.get(
				"https://api.twitter.com/1.1/statuses/user_timeline.json?screen_name={screen_name}&count={count}&max_id={max_id}")
				.routeParam("screen_name", screen_name).routeParam("count", "" + request_twetcount)
				.routeParam("max_id", "" + (max_id - 1)).header("Authorization", "Bearer " + bearer_token);
		System.out.println("Request " + request_twetcount + " tweets to: " + getReq.getUrl());
		json_str_Response = getReq.asStringAsync(this);
	}

	public void requestTweets() throws InterruptedException {
		/*
		 * si los tweets caben en una sóla petición, o bien se piden más de
		 * MAX_TWEET_COUNT_PER_REQUEST en principio la primera petición hay que hacerla
		 * para obtener el max_id (máximo id de la secuencia de tweets)
		 */
		synchronized (wait_inner) {

			pending = tweetcount;
			long max_id = Long.MAX_VALUE;
			nextRequest();
			synchronized (wait_outer) {
				wait_outer.wait();
			}
		}

	}

	public List<Tweet> getTweets() {
		return tweet_list_total;
	}

	public void finish() {
		System.out.println("notify outer");
		synchronized (wait_outer) {

			wait_outer.notify();
		}
	}

	public void ShutDown() {
		Unirest.shutDown();
	}

	

}
