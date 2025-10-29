/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.michaelkoepf.spegauge.generators.nexmark.generator;

// import org.apache.flink.table.utils.ThreadLocalCache;

import net.michaelkoepf.spegauge.api.common.model.nexmark.Bid;
import net.michaelkoepf.spegauge.generators.nexmark.util.GeneratorConfig;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.statistics.distribution.ContinuousDistribution.Sampler;

import java.time.Instant;

/** Generates bids. */
public class BidGenerator {

  /**
   * Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are 1 over these
   * values.
   */
  private static final int HOT_AUCTION_RATIO = 100;

  private static final int HOT_BIDDER_RATIO = 100;

  private static final int HOT_CHANNELS_RATIO = 2;

  private static final int CHANNELS_NUMBER = 10_000;

  private static final String[] HOT_CHANNELS =
      new String[] {"Google", "Facebook", "Baidu", "Apple"};
  private static final String[] HOT_URLS =
      new String[] {getBaseUrl(), getBaseUrl(), getBaseUrl(), getBaseUrl()};

  //  private static final ThreadLocalCache<Integer, Tuple2<String, String>> CHANNEL_URL_CACHE =
  //          new ThreadLocalCache<Integer, Tuple2<String, String>>(CHANNELS_NUMBER) {
  //
  //            @Override
  //            public Tuple2<String, String> getNewInstance(Integer channelNumber) {
  //              String url = getBaseUrl();
  //              if (new Random().nextInt(10) > 0) {
  //                url = url + "&channel_id=" + Math.abs(Integer.reverse(channelNumber));
  //              }
  //              return new Tuple2<>("channel-" + channelNumber, url);
  //            }
  //  };

  /** Generate and return a random bid with next available id with uniform distribution on the auction and person id. */
  public static Bid nextBid(long eventId, UniformRandomProvider random, long timestamp, GeneratorConfig config) {

    long auction;
    auction = AuctionGenerator.nextBase0AuctionId(eventId, random, config);
    auction += GeneratorConfig.FIRST_AUCTION_ID;

    long bidder;
    bidder = PersonGenerator.nextBase0PersonId(eventId, random, config);
    bidder += GeneratorConfig.FIRST_PERSON_ID;

    long price = PriceGenerator.nextPrice(random);

    //    String channel;
    //    String url;
    //    if (random.nextInt(HOT_CHANNELS_RATIO) > 0) {
    //      int i = random.nextInt(HOT_CHANNELS.length);
    //      channel = HOT_CHANNELS[i];
    //      url = HOT_URLS[i];
    //    } else {
    //      Tuple2<String, String> channelAndUrl =
    // CHANNEL_URL_CACHE.get(random.nextInt(CHANNELS_NUMBER));
    //      channel = channelAndUrl.f0;
    //      url = channelAndUrl.f1;
    //    }

    int currentSize = 8 + 8 + 8 + 8;
    String extra = StringsGenerator.nextExtra(random, currentSize, config.getAvgBidByteSize());
    return new Bid(
        auction, bidder, price, /*channel, url,*/ Instant.ofEpochMilli(timestamp), extra);
  }

  /** Generate and return a random bid with next available id with the desired distribution on the auction id. */
  public static Bid nextBid(long eventId, UniformRandomProvider random, Sampler sampler, long timestamp, GeneratorConfig config) {

    long auction;
    auction = AuctionGenerator.nextBase0AuctionId(eventId, sampler, config);
    auction += GeneratorConfig.FIRST_AUCTION_ID;

    long bidder;
    bidder = PersonGenerator.nextBase0PersonId(eventId, random, config);
    bidder += GeneratorConfig.FIRST_PERSON_ID;

    long price = PriceGenerator.nextPrice(random);

    //    String channel;
    //    String url;
    //    if (random.nextInt(HOT_CHANNELS_RATIO) > 0) {
    //      int i = random.nextInt(HOT_CHANNELS.length);
    //      channel = HOT_CHANNELS[i];
    //      url = HOT_URLS[i];
    //    } else {
    //      Tuple2<String, String> channelAndUrl =
    // CHANNEL_URL_CACHE.get(random.nextInt(CHANNELS_NUMBER));
    //      channel = channelAndUrl.f0;
    //      url = channelAndUrl.f1;
    //    }

    int currentSize = 8 + 8 + 8 + 8;
    String extra = StringsGenerator.nextExtra(random, currentSize, config.getAvgBidByteSize());
    return new Bid(
            auction, bidder, price, /*channel, url,*/ Instant.ofEpochMilli(timestamp), extra);
  }

  private static String getBaseUrl() {
//    Random random = new Random();
//    return "https://www.nexmark.com/"
//        + nextString(random, 5, '_')
//        + '/'
//        + nextString(random, 5, '_')
//        + '/'
//        + nextString(random, 5, '_')
//        + '/'
//        + "item.htm?query=1";
    return null;
  }
}
