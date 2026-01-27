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

import java.io.Serializable;

/** An auction submitted by a person.
 * Average size 500 bytes
 */
public class Auction implements Serializable {

    /** ID of auction. */
    public long id; // primary key

    /** Extra auction properties. */
    //public String itemName;

    public String description;

    /** Initial bid price, in cents. */
    public long initialBid;

    /** Reserve price, in cents. */
    public long reserve;

    public long dateTime;

    /** When does auction expire? (ms since epoch). Bids at or after this time are ignored. */
    public long expires;

    /** Id of person who instigated auction. */
    public long seller; // foreign key: Person.id

    /** Id of category auction is listed under. */
    public long category; // foreign key: Category.id

    /** Additional arbitrary payload for performance testing. */
    //public String extra;

    public Auction() {
    }

    public Auction(
            long id,
            String description,
            long initialBid,
            long reserve,
            long dateTime,
            long expires,
            long seller,
            long category) {
        this.id = id;
        this.description = description;
        this.initialBid = initialBid;
        this.reserve = reserve;
        this.dateTime = dateTime;
        this.expires = expires;
        this.seller = seller;
        this.category = category;
    }

    public Auction(String[] attrs) {
        this.id = Long.parseLong(attrs[4]);
        if (id < 0) {
            throw new IllegalArgumentException("Auction ID must be non-negative, but was: " + id);
        }
        this.dateTime = Long.parseLong(attrs[2]);
        this.seller = Long.parseLong(attrs[9]);
        this.category = Long.parseLong(attrs[8]);
        this.expires = Long.parseLong(attrs[7]);
        this.initialBid = Long.parseLong(attrs[5]);
        this.reserve = Long.parseLong(attrs[1]);
        this.description = attrs[11];
    }
}
