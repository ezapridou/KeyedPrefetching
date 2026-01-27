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

package net.michaelkoepf.spegauge.api.common.model.sqbench;

import net.michaelkoepf.spegauge.api.sut.BitSet;

import java.io.Serializable;

/** A bid for an item on auction.
 * Average size 200 bytes
 */
public class Bid implements Serializable {

    /** ID of auction this bid is for. */
    public long auction; // foreign key: Auction.id

    /** ID of person bidding in auction. */
    public long bidder; // foreign key: Person.id

    /** Price of bid, in cents. */
    public long price;

    /**
     * Instant at which bid was made (ms since epoch). NOTE: This may be earlier than the system's
     * event time.
     */
    public long dateTime;

    /** Additional arbitrary payload for performance testing. */
    public String extra;

    public Bid() {

    }

    public Bid(
            long auction,
            long bidder,
            long price,
            long dateTime,
            String extra) {
        this.auction = auction;
        this.bidder = bidder;
        this.price = price;
        this.dateTime = dateTime;
        this.extra = extra;
    }

    public Bid(String[] attrs) {
        this.auction = Long.parseLong(attrs[5]);
        if (auction < 0) {
            throw new IllegalArgumentException("FK Auction ID must be non-negative, but was: " + auction);
        }
        this.bidder = Long.parseLong(attrs[4]);
        this.price = Long.parseLong(attrs[8]);
        this.dateTime = Long.parseLong(attrs[2]);
        this.extra = attrs[11];
    }
}
