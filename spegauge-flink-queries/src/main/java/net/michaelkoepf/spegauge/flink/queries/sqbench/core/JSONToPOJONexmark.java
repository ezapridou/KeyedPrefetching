package net.michaelkoepf.spegauge.flink.queries.sqbench.core;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.michaelkoepf.spegauge.api.common.model.sqbench.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class JSONToPOJONexmark extends ProcessFunction<NexmarkJSONEvent, NexmarkEvent> {

  private final ObjectMapper mapper = new ObjectMapper();
  private boolean keepAuction, keepBid, keepPerson;

    public JSONToPOJONexmark(boolean keepAuction, boolean keepBid, boolean keepPerson) {
        this.keepAuction = keepAuction;
        this.keepBid = keepBid;
        this.keepPerson = keepPerson;
    }

  @Override
  public void processElement(NexmarkJSONEvent value, Context ctx, Collector<NexmarkEvent> out) throws Exception {
    if (keepAuction && value.type == NexmarkEvent.Type.AUCTION) {
      Auction auction = mapper.readValue(value.jsonString, Auction.class);
      out.collect(new NexmarkEvent(auction, value.eventTimeStampMilliSecondsSinceEpoch, value.processingTimeMilliSecondsStart));
    } else if (keepBid && value.type == NexmarkEvent.Type.BID) {
      Bid bid = mapper.readValue(value.jsonString, Bid.class);
      out.collect(new NexmarkEvent(bid, value.eventTimeStampMilliSecondsSinceEpoch, value.processingTimeMilliSecondsStart));
    } else if (keepPerson && value.type == NexmarkEvent.Type.PERSON) {
      Person person = mapper.readValue(value.jsonString, Person.class);
      out.collect(new NexmarkEvent(person, value.eventTimeStampMilliSecondsSinceEpoch, value.processingTimeMilliSecondsStart));
    } else if (value.type == NexmarkEvent.Type.MARKER) {
      out.collect(new NexmarkEvent(NexmarkEvent.Type.MARKER, value.eventTimeStampMilliSecondsSinceEpoch, value.processingTimeMilliSecondsStart));
    }
    //throw new RuntimeException("Unsupported NexmarkEvent type for JSON deserialization: " + value.type);
  }
}