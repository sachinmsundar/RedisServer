package com.reddevil.test.redisHandler;

import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import redis.clients.jedis.Jedis;


/**
 * Created by sachinsomasundar on 8/25/18.
 */
public class RedisVertxServer extends AbstractVerticle{

    @Override
    public void start() {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        Jedis jedis = new Jedis("localhost");
        System.out.println("Redis Host - Accepting Connections..");


            router.route(HttpMethod.POST, "/transactionGet").handler(routingContext -> {
                HttpServerRequest req = routingContext.request();
                    req.bodyHandler(buffer -> {
                        try {
                            String key = buffer.toString().trim();
                            JsonObject resp = new JsonObject();
                            String r = jedis.get(key);

                            //build response
                            resp.put("key", key);
                            if (r == null || r.isEmpty()) {
                                resp.put("value", "");
                            } else {
                                resp.put("value", new JsonObject(r));
                            }
                            System.out.println("RedisServerHandler - Processed get operation for key: " +key);
                            routingContext.response().putHeader("content-type", "application/json").end(Json.encode(resp));
                        }catch (Exception e){
                            System.out.println("RedisServerHandler - Error in get operation..");
                            routingContext.response()
                                    .putHeader("content-type", "application/json")
                                    .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                        }
                    });
            });


        router.route(HttpMethod.POST, "/transactionPut").handler(routingContext -> {
            HttpServerRequest req = routingContext.request();
            try {
                req.bodyHandler(buffer -> {
                    try {
                        JsonObject jObj = buffer.toJsonObject();
                        JsonObject resp = new JsonObject();
                        String key = jObj.getValue("key").toString();
                        String val = jObj.getValue("value").toString();
                        jedis.set(key, val);
                        resp.put("returnCode", "Success");
                        System.out.println("RedisServerHandler - Processed put operation for key: " +key);
                        routingContext.response().putHeader("content-type", "application/json").end(Json.encode(resp));
                    }catch (DecodeException e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }
                });
            }catch (Exception e){
                System.out.println("RedisServerHandler - Error in put operation..");
                routingContext.response()
                        .putHeader("content-type", "application/json")
                        .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
            }
        });


        router.route(HttpMethod.POST, "/transactionDelete").handler(routingContext -> {
            HttpServerRequest req = routingContext.request();
                req.bodyHandler(buffer -> {
                    try {
                        String key = buffer.toString().trim();
                        JsonObject resp = new JsonObject();
                        if (jedis.get(key) != null) {
                            jedis.del(key);
                            resp.put("returnCode", "Success");
                        } else {
                            resp.put("returnCode", "KeyNotFound");
                        }
                        System.out.println("RedisServerHandler - Processed in delete operation for key: " +key);
                        routingContext.response().putHeader("content-type", "application/json").end(Json.encode(resp));
                    }catch (Exception e){
                        System.out.println("RedisServerHandler - Error in delete operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }
                });
        });

        router.route(HttpMethod.POST, "/transactionDelete").handler(routingContext -> {
            HttpServerRequest req = routingContext.request();
            req.bodyHandler(buffer -> {
                try {
                    String key = buffer.toString().trim();
                    JsonObject resp = new JsonObject();
                    if (jedis.get(key) != null) {
                        jedis.del(key);
                        resp.put("returnCode", "Success");
                    } else {
                        resp.put("returnCode", "KeyNotFound");
                    }
                    System.out.println("RedisServerHandler - Processed in delete operation for key: " +key);
                    routingContext.response().putHeader("content-type", "application/json").end(Json.encode(resp));
                }catch (Exception e){
                    System.out.println("RedisServerHandler - Error in delete operation..");
                    routingContext.response()
                            .putHeader("content-type", "application/json")
                            .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                }
            });
        });

        //endpoint for adding user to the active user table
        router.route(HttpMethod.POST, "/transactionAddUser").handler(routingContext -> {
            HttpServerRequest req = routingContext.request();
            try {
                req.bodyHandler(buffer -> {
                    try {
                        JsonObject jObj = buffer.toJsonObject();
                        JsonObject resp = new JsonObject();
                        String key = jObj.getValue("key").toString();
                        String val = jObj.getValue("value").toString();
//                        JsonArray activeUsers = (JsonArray) jObj.getValue("value");

                        JsonObject inputJ = new JsonObject(val);

                        String inputkey = inputJ.getJsonArray("userList").getJsonObject(0).getString("userKey");
                        //check is key exists
                        String r = jedis.get(key);
                        JsonObject jo = null;

                        //build response
                        if (r == null || r.isEmpty()) {
                            jedis.set(key, val);
                            resp.put("returnCode", "Success");
                        } else {
                            jo = new JsonObject(r);
                        }

                        if (jo != null) {
                            boolean added = false;
                            if (jo != null) {
                                JsonArray ja = jo.getJsonArray("userList");
                                for (int i = 0; i < ja.size(); i++) {
                                    JsonObject obj = ja.getJsonObject(i);
                                    String uKey = obj.getString("userKey");
                                    if (uKey.equals(inputkey)) {
                                        ja.add(val);
                                        jo.put("userList", ja);
                                        added = true;
                                    }
                                }

                                if (added == false) {
                                    ja.add(inputJ.getJsonArray("userList").getJsonObject(0));
                                    jo.put("userList", ja);
                                }
                            }

                            //put the updated list back
                            jedis.set(key, jo.toString());

                            resp.put("returnCode", "Success");
                        }

                        System.out.println("RedisServerHandler - Processed put operation for key: " +key);
                        routingContext.response().putHeader("content-type", "application/json").end(Json.encode(resp));
                    }catch (DecodeException e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }
                });
            }catch (Exception e){
                System.out.println("RedisServerHandler - Error in put operation..");
                routingContext.response()
                        .putHeader("content-type", "application/json")
                        .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
            }
        });


        //endpoint for delete from the active user table..
        router.route(HttpMethod.POST, "/transactionDeleteUser").handler(routingContext -> {
            HttpServerRequest req = routingContext.request();
            try {
                req.bodyHandler(buffer -> {
                    try {
                        String key = buffer.toString().trim();
                        JsonObject resp = new JsonObject();
//                        String key = jObj.getValue("key").toString();
//                        String val = jObj.getJsonObject("value").toString();
//                        JsonArray activeUsers = (JsonArray) jObj.getValue("value");

//                        JsonObject inputJ = new JsonObject(val);
//                        String inputkey = inputJ.getString("userKey");
                        //check is key exists
                        String r = jedis.get("activeUsers");
                        JsonObject jo = null;
                        boolean keyFound = false;

                        //build response
                        if (r == null || r.isEmpty()) {
                            resp.put("Error", "User Not Active");

                        } else {
                            jo = new JsonObject(r);
                        }

                        if (jo != null) {
                            JsonArray ja = new JsonArray();
                            if (jo != null) {
                                ja = jo.getJsonArray("userList");
                                for (int i = 0; i < ja.size(); i++) {
                                    JsonObject obj = ja.getJsonObject(i);
                                    String uKey = obj.getString("userKey");
                                    if (uKey.equals(key)) {
                                        ja.remove(i);
                                        jo.put("userList", ja);
                                        keyFound = true;
                                    }
                                }
                            }
                            //put the updated list back
                            if (keyFound) {
                                jedis.set("activeUsers", jo.toString());
                                resp.put("returnCode", "Success");
                            } else {
                                resp.put("Error", "User Not Active");
                            }
                        }

                        System.out.println("RedisServerHandler - Processed put operation for key: " +key);
                        routingContext.response().putHeader("content-type", "application/json").end(Json.encode(resp));
                    }catch (DecodeException e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }
                });
            }catch (Exception e){
                System.out.println("RedisServerHandler - Error in put operation..");
                routingContext.response()
                        .putHeader("content-type", "application/json")
                        .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
            }
        });


        //endpoint for transaction complete..
        router.route(HttpMethod.POST, "/transactionComplete").handler(routingContext -> {
            HttpServerRequest req = routingContext.request();
            try {
                req.bodyHandler(buffer -> {
                    try {
                        JsonObject jObj = buffer.toJsonObject();
                        JsonObject resp = new JsonObject();
                        String key = jObj.getValue("key").toString()+"history";
                        String val = jObj.getValue("value").toString();

                        JsonObject inputJ = new JsonObject(val);

                        String inputkey = inputJ.getJsonArray("txnHistory").getJsonObject(0).toString();
                        //check is key exists
                        String r = jedis.get(key);
                        JsonObject jo = null;

                        //build response
                        if (r == null || r.isEmpty()) {
                            jedis.set(key, val);
                            resp.put("returnCode", "Success");
                        } else {
                            jo = new JsonObject(r);
                        }

                        if (jo != null) {
                            boolean added = false;
                            if (jo != null) {
                                JsonArray ja = jo.getJsonArray("txnHistory");

                                ja.add(inputJ.getJsonArray("txnHistory").getJsonObject(0));
                                jo.put("txnHistory", ja);
                            }

                            //put the updated list back
                            jedis.set(key, jo.toString());
                            resp.put("returnCode", "Success");
                        }

                        System.out.println("RedisServerHandler - Processed put operation for key: " +key);
                        routingContext.response().putHeader("content-type", "application/json").end(Json.encode(resp));
                    }catch (DecodeException e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }
                });
            }catch (Exception e){
                System.out.println("RedisServerHandler - Error in put operation..");
                routingContext.response()
                        .putHeader("content-type", "application/json")
                        .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
            }
        });


        server.requestHandler(router::accept).listen(8080);
    }
}
