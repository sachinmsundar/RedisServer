package com.reddevil.test.redisHandler;

import io.vertx.core.VertxException;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import redis.clients.jedis.Jedis;

import java.util.UUID;


/**
 * Created by sachinsomasundar on 8/25/18.
 */
public class RedisVertxServer extends AbstractVerticle{

    @Override
    public void start() throws Exception {
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
                    }catch (NullPointerException e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }catch (Exception e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }
                });
            }catch(VertxException e){
                System.out.println("RedisServerHandler - Error in put operation..");
                routingContext.response()
                        .putHeader("content-type", "application/json")
                        .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
            }catch (NullPointerException e){
                System.out.println("RedisServerHandler - Error in put operation..");
                routingContext.response()
                        .putHeader("content-type", "application/json")
                        .end(Json.encode(new JsonObject().put("Error", e.getMessage().toString())));
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
                        System.out.println("RedisServerHandler - Processed delete operation for key: " +key);
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
                    System.out.println("RedisServerHandler - Processed delete operation for key: " +key);
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
                        JsonArray forUnni = new JsonArray();

                        //build response
                        if (r == null || r.isEmpty()) {
                            jedis.set(key, val);
                            resp.put("returnCode", "Success");
                        } else {
                            jo = new JsonObject(r);
                        }

                        try {
                            if (jo != null) {
                                boolean added = false;
                                if (jo != null) {
                                    JsonArray ja = jo.getJsonArray("userList");
                                    for (int i = 0; i < ja.size(); i++) {
                                        JsonObject obj = ja.getJsonObject(i);
                                        String uKey = obj.getString("userKey");
                                        //forUnni.add()
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

                                    //call unni
                                    JsonArray matched = null;
                                    if (ja.size() > 1) {
                                        matched = matchUsers(ja);
                                        UUID uuid = UUID.randomUUID();
                                        String randomUUIDString = uuid.toString();

                                        if (matched.size() == 2) {
                                            JsonObject first = matched.getJsonObject(0);
                                            JsonObject second = matched.getJsonObject(1);

                                            JsonObject m1 = new JsonObject();
                                            JsonObject m2 = new JsonObject();

                                            m1.put("matchedTo", second.getString("userKey"));
                                            m1.put("rideID", randomUUIDString);
                                            m1.put("rideType", first.getString("rideType"));
                                            m1.put("startLat", first.getString("startLat"));
                                            m1.put("startLong", first.getString("startLong"));
                                            m1.put("destLat", first.getString("destLat"));
                                            m1.put("destLong", first.getString("destLong"));


                                            m1.put("startLatOther", second.getString("startLat"));
                                            m1.put("startLongOther", second.getString("startLong"));
                                            m1.put("destLatOther", second.getString("destLat"));
                                            m1.put("destLongOther", second.getString("destLong"));


                                            m2.put("matchedTo", first.getString("userKey"));
                                            m2.put("rideID", randomUUIDString);
                                            m2.put("rideType", second.getString("rideType"));

                                            m2.put("startLat", second.getString("startLat"));
                                            m2.put("startLong", second.getString("startLong"));
                                            m2.put("destLat", second.getString("destLat"));
                                            m2.put("destLong", second.getString("destLong"));

                                            m2.put("startLatOther", first.getString("startLat"));
                                            m2.put("startLongOther", first.getString("startLong"));
                                            m2.put("destLatOther", first.getString("destLat"));
                                            m2.put("destLongOther", first.getString("destLong"));

                                            String k1 = first.getString("userKey") + "matched";
                                            String k2 = second.getString("userKey") + "matched";

                                            jedis.set(k1, m1.toString());
                                            jedis.set(k2, m2.toString());

                                            //remove these two users from the active users table
                                            String remove1 = first.getString("userKey");
                                            String remove2 = second.getString("userKey");
                                            int size = ja.size();
                                            JsonArray newJa = new JsonArray();
                                            for (int i = 0; i < size; i++) {
                                                JsonObject obj = ja.getJsonObject(i);
                                                String uKey = obj.getString("userKey");
                                                if (uKey.equals(remove1) || uKey.equals(remove2)) {
//                                                ja.remove(i);
                                                } else {
                                                    newJa.add(obj);
                                                }
                                            }
                                            jo.put("userList", newJa);
                                        }
                                    }

                                }

                                //put the updated list back
                                jedis.set(key, jo.toString());

                                resp.put("returnCode", "Success");
                            }
                        }catch(Exception e){
                            System.out.println("RedisServerHandler - Error in put operation..");
                            routingContext.response()
                                    .putHeader("content-type", "application/json")
                                    .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                        }

                        System.out.println("RedisServerHandler - Processed put operation for key: " +key);
                        routingContext.response().putHeader("content-type", "application/json").end(Json.encode(resp));
                    }catch (DecodeException e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }catch (NullPointerException e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }catch(VertxException e){
                        System.out.println("RedisServerHandler - Error in put operation..");
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(new JsonObject().put("Error", e.getMessage())));
                    }catch(Exception e){
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
                    }catch (Exception e){
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
                    }catch(Exception e){
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

    public JsonArray matchUsers(JsonArray users){

        JsonArray resp = new JsonArray();
        boolean req = false;
        boolean off = false;
        for(int i = 0; i < users.size(); i++){
            JsonObject j = users.getJsonObject(i);

            if ((users.getJsonObject(i).getString("rideType").equals("Request"))){
                if(req == false){
                    resp.add(users.getJsonObject(i));
                    req = true;
                }
            }

            if ((users.getJsonObject(i).getString("rideType").equals("Offer"))){
                if(off == false){
                    resp.add(users.getJsonObject(i));
                    off = true;
                }
            }
        }

        return resp;
    }
}
