package com.reddevil.test.redisHandler;

import com.fasterxml.jackson.databind.util.JSONPObject;
import io.vertx.core.json.JsonObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by sachinsomasundar on 8/28/18.
 */
public class HttpPostRequest {
    public static void main(String[] args) throws IOException {

        URL url = new URL("http://localhost:8080/transactionPut");
        HttpURLConnection http = (HttpURLConnection) url.openConnection();

        http.setDoOutput(true);
        http.setDoInput(true);
        http.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        http.setRequestProperty("Accept", "application/json");
        http.setRequestMethod("POST");

        JsonObject req = new JsonObject("{\n" +
                "\t\"key\": \"test1\",\n" +
                "\n" +
                "\t\"value\": {\n" +
                "\t\t\"name\": \"Sac\",\n" +
                "\t\t\"HomeAddr\": \"Eset\",\n" +
                "\t\t\"OfficeAddr\": \"ABCD\",\n" +
                "\t\t\"StartTime\": \"0900\",\n" +
                "\t\t\"ReturnTime\": \"1700\",\n" +
                "\t\t\"Vehicle\": \"Ferrari\"\n" +
                "\t}\n" +
                "}");

        OutputStreamWriter wr = new OutputStreamWriter(http.getOutputStream());
        wr.write(req.toString());
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
    }
}
