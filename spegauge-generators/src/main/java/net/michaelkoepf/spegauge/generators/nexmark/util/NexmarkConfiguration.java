// TODO: check how to reference properly
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

package net.michaelkoepf.spegauge.generators.nexmark.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;

public class NexmarkConfiguration implements Serializable {

  /**
   * Comments in the JSON file since JSON does not allow comments.
   */
  @JsonIgnore
  public String _comments;

  /**
   * Number of events to generate. If zero, generate as many as possible without overflowing
   * internal counters etc.
   */
  @JsonProperty public long numEvents = 0;

  /** Number of event generators to use. Each generates events in its own timeline. */
  @JsonProperty public int numEventGenerators = 1;

  /** Shape of event rate curve. */
  @JsonProperty public NexmarkUtils.RateShape rateShape = NexmarkUtils.RateShape.SQUARE;

  /** Initial overall event rate (in {@link #rateUnit}). */
  @JsonProperty public int firstEventRate = 10000;

  /** Next overall event rate (in {@link #rateUnit}). */
  @JsonProperty public int nextEventRate = 10000;

  /** Unit for rates. */
  @JsonProperty public NexmarkUtils.RateUnit rateUnit = NexmarkUtils.RateUnit.PER_SECOND;

  /** Overall period of rate shape, in seconds. */
  @JsonProperty public int ratePeriodSec = 600;

  /**
   * Time in seconds to preload the subscription with data, at the initial input rate of the
   * pipeline.
   */
  @JsonProperty public int preloadSeconds = 0;

  /** Timeout for stream pipelines to stop in seconds. */
  @JsonProperty public int streamTimeout = 240;

  /**
   * If true, and in streaming mode, generate events only when they are due according to their
   * timestamp.
   */
  @JsonProperty public boolean isRateLimited = false;

  /**
   * If true, use wallclock time as event time. Otherwise, use a deterministic time in the past so
   * that multiple runs will see exactly the same event streams and should thus have exactly the
   * same results.
   */
  @JsonProperty public boolean useWallclockEventTime = false;

  /** Person Proportion. */
  @JsonProperty public int personProportion = 40;

  /** Auction Proportion. */
  @JsonProperty public int auctionProportion = 50;

  /** Bid Proportion. */
  @JsonProperty public int bidProportion = 10;

  /** Average idealized size of a 'new person' event, in bytes. */
  @JsonProperty public int avgPersonByteSize = 200;

  /** Average idealized size of a 'new auction' event, in bytes. */
  @JsonProperty public int avgAuctionByteSize = 500;

  /** Average idealized size of a 'bid' event, in bytes. */
  @JsonProperty public int avgBidByteSize = 100;

  /** Ratio of bids to 'hot' auctions compared to all other auctions. */
  @JsonProperty public int hotAuctionRatio = 1;

  /** Ratio of auctions for 'hot' sellers compared to all other people. */
  @JsonProperty public int hotSellersRatio = 128;

  /** Ratio of bids for 'hot' bidders compared to all other people. */
  @JsonProperty public int hotBiddersRatio = 4;

  /** Number of categories */
  @JsonProperty public int numCategories = 4;

  /** Number of seconds to hold back events according to their reported timestamp. */
  @JsonProperty public long watermarkHoldbackSec = 0;

  /** Average number of auction which should be inflight at any time, per generator. */
  @JsonProperty public int numInFlightAuctions = 10;

  /** Maximum number of people to consider as active for placing auctions or bids. */
  @JsonProperty public int numActivePeople = 1000;

  /** Length of occasional delay to impose on events (in seconds). */
  @JsonProperty public long occasionalDelaySec = 0;

  /** Probability that an event will be delayed by delayS. */
  @JsonProperty public double probDelayedEvent = 0.0;

  /**
   * Number of events in out-of-order groups. 1 implies no out-of-order events. 1000 implies every
   * 1000 events per generator are emitted in pseudo-random order.
   */
  @JsonProperty public long outOfOrderGroupSize = 1;

  // seed
  @JsonProperty public long seed = 42;

  @Deprecated
  public void update(Properties properties) {
    numEvents = Long.parseLong(properties.getProperty("number.of.events", Long.toString(numEvents)));
    numEventGenerators = Integer.parseInt(properties.getProperty("number.of.event.generators", Integer.toString(numEventGenerators)));
    firstEventRate = Integer.parseInt(properties.getProperty("first.event.rate", Integer.toString(firstEventRate)));
    nextEventRate = Integer.parseInt(properties.getProperty("next.event.rate", Integer.toString(nextEventRate)));
    ratePeriodSec = Integer.parseInt(properties.getProperty("rate.period.seconds", Integer.toString(ratePeriodSec)));
    preloadSeconds = Integer.parseInt(properties.getProperty("preload.seconds", Integer.toString(preloadSeconds)));
    streamTimeout = Integer.parseInt(properties.getProperty("stream.timeout", Integer.toString(streamTimeout)));
    isRateLimited = Boolean.parseBoolean(properties.getProperty("is.rate.limited", Boolean.toString(isRateLimited)));
    useWallclockEventTime =
        Boolean.parseBoolean(properties.getProperty("use.wallclock.event.time", Boolean.toString(useWallclockEventTime)));
    personProportion = Integer.parseInt(properties.getProperty("person.proportion", Integer.toString(personProportion)));
    auctionProportion = Integer.parseInt(properties.getProperty("auction.proportion", Integer.toString(auctionProportion)));
    bidProportion = Integer.parseInt(properties.getProperty("bid.proportion", Integer.toString(bidProportion)));
    avgPersonByteSize = Integer.parseInt(properties.getProperty("avg.person.byte.size", Integer.toString(avgPersonByteSize)));
    avgAuctionByteSize = Integer.parseInt(properties.getProperty("avg.auction.byte.size", Integer.toString(avgAuctionByteSize)));
    avgBidByteSize = Integer.parseInt(properties.getProperty("avg.bid.byte.size", Integer.toString(avgBidByteSize)));
    hotAuctionRatio = Integer.parseInt(properties.getProperty("hot.auction.ratio", Integer.toString(hotAuctionRatio)));
    hotSellersRatio = Integer.parseInt(properties.getProperty("hot.sellers.ratio", Integer.toString(hotSellersRatio)));
    hotBiddersRatio = Integer.parseInt(properties.getProperty("hot.bidders.ratio", Integer.toString(hotBiddersRatio)));
    numCategories = Integer.parseInt(properties.getProperty("number.of.categories", Integer.toString(numCategories)));
    watermarkHoldbackSec = Long.parseLong(properties.getProperty("watermark.holdback.seconds", Long.toString(watermarkHoldbackSec)));
    numInFlightAuctions = Integer.parseInt(properties.getProperty("number.of.in.flight.auctions", Integer.toString(numInFlightAuctions)));
    numActivePeople = Integer.parseInt(properties.getProperty("number.of.active.people", Integer.toString(numActivePeople)));
    occasionalDelaySec = Long.parseLong(properties.getProperty("occasional.delay.seconds", Long.toString(occasionalDelaySec)));
    probDelayedEvent = Double.parseDouble(properties.getProperty("probability.delayed.event", Double.toString(probDelayedEvent)));
    outOfOrderGroupSize = Long.parseLong(properties.getProperty("out.of.order.group.size", Long.toString(outOfOrderGroupSize)));
    seed = Long.parseLong(properties.getProperty("seed", Long.toString(seed)));
  }

  /** Return full description as a string. */
  @Override
  public String toString() {
    try {
      return NexmarkUtils.MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NexmarkConfiguration that = (NexmarkConfiguration) o;
    return numEvents == that.numEvents
        && numEventGenerators == that.numEventGenerators
        && firstEventRate == that.firstEventRate
        && nextEventRate == that.nextEventRate
        && ratePeriodSec == that.ratePeriodSec
        && preloadSeconds == that.preloadSeconds
        && streamTimeout == that.streamTimeout
        && isRateLimited == that.isRateLimited
        && useWallclockEventTime == that.useWallclockEventTime
        && personProportion == that.personProportion
        && auctionProportion == that.auctionProportion
        && bidProportion == that.bidProportion
        && avgPersonByteSize == that.avgPersonByteSize
        && avgAuctionByteSize == that.avgAuctionByteSize
        && avgBidByteSize == that.avgBidByteSize
        && hotAuctionRatio == that.hotAuctionRatio
        && hotSellersRatio == that.hotSellersRatio
        && hotBiddersRatio == that.hotBiddersRatio
        && numCategories == that.numCategories
        && watermarkHoldbackSec == that.watermarkHoldbackSec
        && numInFlightAuctions == that.numInFlightAuctions
        && numActivePeople == that.numActivePeople
        && occasionalDelaySec == that.occasionalDelaySec
        && Double.compare(that.probDelayedEvent, probDelayedEvent) == 0
        && outOfOrderGroupSize == that.outOfOrderGroupSize
        && rateShape == that.rateShape
        && rateUnit == that.rateUnit
        && seed == that.seed;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        numEvents,
        numEventGenerators,
        rateShape,
        firstEventRate,
        nextEventRate,
        rateUnit,
        ratePeriodSec,
        preloadSeconds,
        streamTimeout,
        isRateLimited,
        useWallclockEventTime,
        personProportion,
        auctionProportion,
        bidProportion,
        avgPersonByteSize,
        avgAuctionByteSize,
        avgBidByteSize,
        hotAuctionRatio,
        hotSellersRatio,
        hotBiddersRatio,
        numCategories,
        watermarkHoldbackSec,
        numInFlightAuctions,
        numActivePeople,
        occasionalDelaySec,
        probDelayedEvent,
        outOfOrderGroupSize,
        seed);
  }
}
