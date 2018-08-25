package com.reddevil.test.redisHandler;

import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
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

        server.requestHandler(router::accept).listen(8080);
    }
}
