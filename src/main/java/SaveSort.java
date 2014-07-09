import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.vertx.java.core.json.JsonObject;
import org.xbib.elasticsearch.action.river.state.RiverState;
import org.xbib.elasticsearch.action.river.state.RiverStateAction;
import org.xbib.elasticsearch.action.river.state.RiverStateRequest;
import org.xbib.elasticsearch.action.river.state.RiverStateResponse;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

/**
 * Created by p14n on 10/06/2014.
 */
public class SaveSort {
    public static void main(String args[]) throws IOException {
        Node node = NodeBuilder.nodeBuilder().data(true).local(true).node();

        Client c = node.client();
        saveSort(c);
        node.close();
    }

    private static void saveSort(Client c) throws IOException {
        c.admin().cluster().prepareHealth().setWaitForActiveShards(1).execute().actionGet();

        insertRows(c,0);
        selectRows(c, 0);
        insertRows(c,20);
        selectRows(c,20);
        System.out.println((c.admin().cluster().prepareClusterStats().execute()
                .actionGet().getIndicesStats().getStore().getSizeInBytes() / 1024) + "K");
    }

    private static void selectRows(Client c,int startRow) {
        for (int rs = 0; rs < 4000; rs++) {
            long start = System.currentTimeMillis();
            QueryBuilder queryStringBuilder =
                    QueryBuilders.queryString("rs:" + ( startRow + (rs%20)));

            SearchRequestBuilder requestBuilder =
                    c.prepareSearch("pages")
                            .setTypes("row")
                            .setQuery(queryStringBuilder)
                            .addSort("rowcount", SortOrder.DESC)
                    .setSize(10).setFrom(100);

            SearchResponse response = requestBuilder.execute().actionGet();
            SearchHit s = response.getHits().getAt(1);
            System.out.print(s.getSource().get("rowcount")+" ");
            System.out.println(response.getHits().hits().length + " of " + response.getHits().getTotalHits() + " in " + ((System.currentTimeMillis() - start)));


        }
    }

    private static void insertRows(Client c,int startRow) throws IOException {

        for (int rs = startRow; rs < (startRow+20); rs++) {
            BulkRequestBuilder bulkRequest = c.prepareBulk();
            long start = System.currentTimeMillis();

            for (int i = 0; i < 1000; i++) {
                bulkRequest.add(c.prepareIndex("pages", "row")
                                .setSource(new JsonObject().putString("user1", "kimchy")
                                        .putString("user2", "kimchy")
                                        .putString("user3", "kimchy")
                                        .putString("user4", "kimchy")
                                        .putString("user5", "kimchy")
                                        .putString("user6", "kimchy")
                                        .putString("user7", "kimchy")
                                        .putString("user8", "kimchy")
                                        .putString("user9", "kimchy")
                                        .putNumber("rowcount", i)
                                        .putNumber("rs", rs)
                                        .encode())
                );
            }
            System.out.println("Rows preparation took " + (System.currentTimeMillis() - start));
            BulkResponse bulkResponse = bulkRequest.setRefresh(true).execute().actionGet();
            if (bulkResponse.hasFailures()) {
                System.out.println(bulkResponse);
                // process failures by iterating through each bulk response item
            }
            System.out.println("Rows insertion took " + (System.currentTimeMillis() - start));

        }

    }
}
