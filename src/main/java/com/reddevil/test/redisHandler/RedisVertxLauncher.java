package com.reddevil.test.redisHandler;

import io.vertx.core.Vertx;

/**
 * Created by sachinsomasundar on 8/25/18.
 */
public class RedisVertxLauncher {
    public static void main(String[] args) {
        try {
            Vertx.vertx().deployVerticle(new RedisVertxServer());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
