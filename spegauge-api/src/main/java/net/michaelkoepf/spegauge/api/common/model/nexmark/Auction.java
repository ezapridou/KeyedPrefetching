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

import java.io.Serializable;
import java.time.Instant;

/** An auction submitted by a person. */
// TODO: maybe replace with Apache Avro or similar?
public class Auction implements Serializable {

  /** Id of auction. */
  public long id; // primary key

  /** Extra auction properties. */
  public String itemName;

  public String description;

  /** Initial bid price, in cents. */
  public long initialBid;

  /** Reserve price, in cents. */
  public long reserve;

  public Instant dateTime;

  /** When does auction expire? (ms since epoch). Bids at or after this time are ignored. */
  public Instant expires;

  /** Id of person who instigated auction. */
  public long seller; // foreign key: Person.id

  /** Id of category auction is listed under. */
  public long category; // foreign key: Category.id

  /** Additional arbitrary payload for performance testing. */
  public String extra;

  public boolean sellBelowReserve;

  public Auction() {
  }

  public Auction(
      long id,
      String itemName,
      String description,
      long initialBid,
      long reserve,
      Instant dateTime,
      Instant expires,
      long seller,
      long category,
      String extra,
      boolean sellBelowReserve) {
    this.id = id;
    this.itemName = itemName;
    this.description = description;
    this.initialBid = initialBid;
    this.reserve = reserve;
    this.dateTime = dateTime;
    this.expires = expires;
    this.seller = seller;
    this.category = category;
    this.extra = extra;
    this.sellBelowReserve = sellBelowReserve;
  }

  public Auction(String[] attrs) {
    this.id = Long.parseLong(attrs[1]);
    this.itemName = attrs[2];
    this.description = attrs[3];
    this.initialBid = Long.parseLong(attrs[4]);
    this.reserve = Long.parseLong(attrs[5]);
    this.dateTime = Instant.parse(attrs[6]);
    this.expires = Instant.parse(attrs[7]);
    this.seller = Long.parseLong(attrs[8]);
    this.category = Long.parseLong(attrs[9]);
    this.extra = attrs[10];
    this.sellBelowReserve = attrs[11].equals("true");
  }

  @Override
  public String toString() {
    return "Auction{"
        + "id="
        + id
        + ", itemName='"
        + itemName
        + '\''
        + ", description='"
        + description
        + '\''
        + ", initialBid="
        + initialBid
        + ", reserve="
        + reserve
        + ", dateTime="
        + dateTime
        + ", expires="
        + expires
        + ", seller="
        + seller
        + ", category="
        + category
        + ", extra='"
        + extra
        + ", sellBelowReserve='"
        + sellBelowReserve
        + '\''
        + '}';
  }

  public String toStringCSV() {
    return "1"
        + // code that signifies that this is an auction
        ","
        + id
        + ","
        + itemName
        + ","
        + description
        + ","
        + initialBid
        + ","
        + reserve
        + ","
        + dateTime
        + ","
        + expires
        + ","
        + seller
        + ","
        + category
        + ","
        + extra
        + ","
        + sellBelowReserve;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Auction auction = (Auction) o;
    return id == auction.id
        && initialBid == auction.initialBid
        && reserve == auction.reserve
        && dateTime.compareTo(auction.dateTime) == 0
        && expires.compareTo(auction.expires) == 0
        && seller == auction.seller
        && category == auction.category
        && itemName.equals(auction.itemName)
        && description.equals(auction.description)
        && extra.equals(auction.extra)
        && sellBelowReserve == auction.sellBelowReserve;
  }

  @Override
  public int hashCode() {
    return ((Long) id).hashCode()
        + itemName.hashCode()
        + description.hashCode()
        + ((Long) initialBid).hashCode()
        + ((Long) reserve).hashCode()
        + dateTime.hashCode()
        + expires.hashCode()
        + ((Long) seller).hashCode()
        + ((Long) category).hashCode()
        + extra.hashCode()
        + ((Boolean) sellBelowReserve).hashCode();
  }
}
