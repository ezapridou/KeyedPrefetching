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

package net.michaelkoepf.spegauge.api.common.model.nexmark;

import java.util.Objects;

/**
 * An event in the auction system, either a (new) {@link Person}, a (new) {@link Auction}, or a
 * {@link Bid}.
 */
// TODO: maybe replace with Apache Avro or similar?
public class Event {

  public Person newPerson = null;
  public Auction newAuction = null;
  public Bid bid = null;

  public long eventTimestamp;

  public Type type;

  /** The type of object stored in this event. * */
  public enum Type {
    PERSON(0),
    AUCTION(1),
    BID(2);

    public final int value;

    Type(int value) {
      this.value = value;
    }
  }

  public Event() {
  }

  public Event(Person newPerson) {
    this.newPerson = newPerson;
    type = Type.PERSON;
    eventTimestamp = newPerson.dateTime.toEpochMilli();
  }

  public Event(Auction newAuction) {
    this.newAuction = newAuction;
    type = Type.AUCTION;
    eventTimestamp = newAuction.dateTime.toEpochMilli();
  }

  public Event(Bid bid) {
    this.bid = bid;
    type = Type.BID;
    eventTimestamp = bid.dateTime.toEpochMilli();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Event event = (Event) o;
    return Objects.equals(newPerson, event.newPerson)
        && Objects.equals(newAuction, event.newAuction)
        && Objects.equals(bid, event.bid)
        && Objects.equals(eventTimestamp, event.eventTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newPerson, newAuction, bid, eventTimestamp);
  }

  @Override
  public String toString() {
    if (newPerson != null) {
      return newPerson.toString();
    } else if (newAuction != null) {
      return newAuction.toString();
    } else if (bid != null) {
      return bid.toString();
    } else {
      throw new RuntimeException("Invalid event");
    }
  }

  public String toStringCSV() {
    if (newPerson != null) {
      return newPerson.toStringCSV();
    } else if (newAuction != null) {
      return newAuction.toStringCSV();
    } else if (bid != null) {
      return bid.toStringCSV();
    } else {
      throw new RuntimeException("Invalid event");
    }
  }
}
