package com.reddevil.test.redisHandler;

import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by sachinsomasundar on 8/28/18.
 */

public class HttpPostWithString {
    public static void main(String[] args) throws IOException {

//        URL url = new URL("http://localhost:8080/transactionGet");
//        HttpURLConnection http = (HttpURLConnection) url.openConnection();
//
//        http.setDoOutput(true);
//        http.setDoInput(true);
//        http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
//        http.setRequestProperty("Accept", "application/json");
//        http.setRequestMethod("POST");
//
//        String r = "test1";
//        OutputStreamWriter wr = new OutputStreamWriter(http.getOutputStream());
//        wr.write(r);
//        wr.flush();
//
//
//        StringBuilder sb = new StringBuilder();
//        int HttpResult = http.getResponseCode();
//        if (HttpResult == HttpURLConnection.HTTP_OK) {
//            BufferedReader br = new BufferedReader(
//                    new InputStreamReader(http.getInputStream(), "utf-8"));
//            String line = null;
//            while ((line = br.readLine()) != null) {
//                sb.append(line + "\n");
//            }
//            br.close();
//            System.out.println("" + sb.toString());
//        } else {
//            System.out.println(http.getResponseMessage());
//        }

//
        JsonObject resp = new JsonObject();

    }

    public JsonObject doTransactionGet(String s) throws IOException {

        JsonObject resp = new JsonObject();
        URL url = new URL("http://localhost:8080/transactionGet");
        HttpURLConnection http = (HttpURLConnection) url.openConnection();

        http.setDoOutput(true);
        http.setDoInput(true);
        http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        http.setRequestProperty("Accept", "application/json");
        http.setRequestMethod("POST");

//        String s = "test1";
        OutputStreamWriter wr = new OutputStreamWriter(http.getOutputStream());
        wr.write(s);
        wr.flush();


        StringBuilder sb = new StringBuilder();
        int HttpResult = http.getResponseCode();
        if (HttpResult == HttpURLConnection.HTTP_OK) {
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(http.getInputStream(), "utf-8"));
            String line = null;
            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
            }
            br.close();
            System.out.println("" + sb.toString());
        } else {
            System.out.println(http.getResponseMessage());
        }

        return resp;
    }

}
