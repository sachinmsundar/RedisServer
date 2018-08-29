package com.amex.distance;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by usankara on 8/29/18.
 */
public class rideblue {

    public JsonArray match(JsonArray users)
    {
        List<String> matched = new ArrayList<>();

       JsonObject match = new JsonObject();
        JsonArray matchArray = new JsonArray();

        int count=0;

        for (int i=0;i<users.size();i++)
        {
            if(count<2) {
                if ((users.getJsonObject(i).getString("rideType").equals("Request")) ||
                        (users.getJsonObject(i).getString("rideType").equals("Offer")) )
                {
                    matchArray.add(users.getJsonObject(i));
                }
            }
        }



     //   match.put("match",matchArray);

        return matchArray;
    }
}
