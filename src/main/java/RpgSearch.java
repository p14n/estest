import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.vertx.java.core.json.JsonObject;

import java.io.File;
import java.io.IOException;

/**
 * Created by p14n on 08/07/2014.
 */
public class RpgSearch {

    public static void main(String args[]) throws IOException {
        Node node = NodeBuilder.nodeBuilder().data(true).local(true).node();
        node.start();

        Client c = node.client();
        c.admin().cluster().prepareHealth().setWaitForActiveShards(1).execute().actionGet();
        saveRpg(c);

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        node.close();
    }

    private static void saveRpg(Client c) throws IOException {
        BulkRequestBuilder bulkRequest = c.prepareBulk();
        long start = System.currentTimeMillis();

        int block = 0;
        for (File f:new File("/Users/p14n/dev/jhc/ws/rpg/F63FIGARO").listFiles()) {
            String s = FileUtils.readFileToString(f);
            int dot = f.getName().indexOf('.');

            JsonObject json = new JsonObject();
            json.putString("name",f.getName().substring(dot+1));
            json.putString("content",s);
            bulkRequest.add(c.prepareIndex("rpg", f.getName().substring(0,dot))
                            .setSource(json.encode())
            );
            if(block++>100){
                block = 0;
                BulkResponse bulkResponse = bulkRequest.setRefresh(true).execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    for(BulkItemResponse r:bulkResponse.getItems()){
                        System.out.println(r.getFailureMessage());
                    }
                }
                System.out.println("Rows insertion took " + (System.currentTimeMillis() - start));
                bulkRequest = c.prepareBulk();
            }
        }
    }
}
