import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.xbib.elasticsearch.action.river.state.RiverState;
import org.xbib.elasticsearch.action.river.state.RiverStateAction;
import org.xbib.elasticsearch.action.river.state.RiverStateRequest;
import org.xbib.elasticsearch.action.river.state.RiverStateResponse;
import org.xbib.elasticsearch.plugin.jdbc.RiverContext;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

/**
 * Created by p14n on 17/06/2014.
 */
public class River {

    public static void main(String args[]) throws Exception {
        Node node = NodeBuilder.nodeBuilder().data(true).local(true).node();
        Client c = node.client();
        river(c);
        node.close();
    }

    private static void river(Client c) throws Exception {
        try {
            c.admin().indices().create(new CreateIndexRequest("_river")).actionGet();
        } catch (IndexAlreadyExistsException e) {
            //e.printStackTrace();
        }
        /*RiverSource source = new SimpleRiverSource()
                .setUrl("jdbc:as400://tracey.servers.jhc.co.uk;naming=system;prompt=false")
                .setUser("DPCDEV")
                .setPassword("DPCDEV");
        RiverContext context = new RiverContext()
                .setRiverSource(source)
                .setRetries(1)
                .setMaxRetryWait(TimeValue.timeValueSeconds(5))
                .setLocale("en");
        context.contextualize();*/


        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("type", "jdbc")
                .field("jdbc", "{\"url\" : \"jdbc:as400://tracey.servers.jhc.co.uk;naming=system;prompt=false\")\"," +
                        "\"user\" : \"DPCDEV\"," +
                        "\"password\" : \"DPCDEV\"," +
                        "\"sql\" : \"select uecode _id,uename from person where uename='ADAM'\"," +
                        "\"schedule\" : \"0/5 0-59 0-23 ? * *\"," +
                        "\"bulk_flush_interval\" : \"1s\"," +
                        "\"index\" : \"my_jdbc_river_index\"," +
                        "\"type\" : \"my_jdbc_river_type\"}")
                .endObject();

        System.out.println(builder.string());

       IndexRequest indexRequest = Requests.indexRequest("_river").type("my_jdbc_river").id("_meta")
                .source(builder.string());
       IndexResponse f =  c.index(indexRequest).actionGet();

        c.admin().indices().prepareRefresh("_river").execute().actionGet();
        waitForRiverEnabled(c, "my_jdbc_river", 15);
        waitForRiverActive(c, "my_jdbc_river", 15);

    }

    public static void waitForRiverEnabled(Client client, String riverName, int seconds) throws InterruptedException, IOException {
        RiverStateRequest riverStateRequest = new RiverStateRequest()
                .setRiverName(riverName);
        RiverStateResponse riverStateResponse = client
                .execute(RiverStateAction.INSTANCE, riverStateRequest).actionGet();
        while (seconds-- > 0 && !isEnabled(riverName, riverStateResponse)) {
            Thread.sleep(1000L);
            try {
                riverStateResponse = client.execute(RiverStateAction.INSTANCE, riverStateRequest).actionGet();
            } catch (IndexMissingException e) {
                // ignore
            }
        }
    }
    public static void waitForRiverActive(Client client, String riverName, int seconds) throws InterruptedException, IOException {
        RiverStateRequest riverStateRequest = new RiverStateRequest()
                .setRiverName(riverName);
        RiverStateResponse riverStateResponse = client
                .execute(RiverStateAction.INSTANCE, riverStateRequest).actionGet();

        seconds *= 10;
        while (seconds-- > 0 && !isActive(riverName, riverStateResponse)) {
            Thread.sleep(100L);
            try {
                riverStateResponse = client.execute(RiverStateAction.INSTANCE, riverStateRequest).actionGet();
            } catch (IndexMissingException e) {
                //
            }
        }
        if (seconds < 0) {
            throw new IOException("timeout waiting for active river");
        }
    }


    private static boolean isEnabled(String riverName, RiverStateResponse riverStateResponse) {
        if (riverStateResponse == null) {
            return false;
        }
        if (riverStateResponse.getStates() == null) {
            return false;
        }
        if (riverStateResponse.getStates().isEmpty()) {
            return false;
        }
        for (RiverState state : riverStateResponse.getStates()) {
            if (state.getName().equals(riverName)) {
                return state.isEnabled();
            }
        }
        return false;
    }
    private static boolean isActive(String riverName, RiverStateResponse riverStateResponse) {
        if (riverStateResponse == null) {
            return false;
        }
        if (riverStateResponse.getStates() == null) {
            return false;
        }
        if (riverStateResponse.getStates().isEmpty()) {
            return false;
        }
        for (RiverState state : riverStateResponse.getStates()) {
            if (state.getName().equals(riverName)) {
                return state.isActive();
            }
        }
        return false;
    }

}
