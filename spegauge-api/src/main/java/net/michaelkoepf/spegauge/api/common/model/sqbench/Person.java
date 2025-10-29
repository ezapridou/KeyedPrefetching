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

/** A person either creating an auction or making a bid.
 *  Average size 200 bytes
 * */
public class Person implements Serializable {

    /** Id of person. */
    public long id; // primary key

    /** Extra person properties. */
    public String name;

    public long creditCard;

    public long city;

    public long state;

    public long dateTime;

    /** Additional arbitrary payload for performance testing. */
    //public String extra;

    public Person() {

    }

    public Person(
            long id,
            String name,
            long creditCard,
            long city,
            long state,
            long dateTime) {
        this.id = id;
        this.name = name;
        this.creditCard = creditCard;
        this.city = city;
        this.state = state;
        this.dateTime = dateTime;
    }

    public Person(String[] attrs) {
        this.id = Long.parseLong(attrs[5]);
        this.name = attrs[11];
        this.creditCard = Long.parseLong(attrs[7]);
        this.city = Long.parseLong(attrs[8]);
        this.state = Long.parseLong(attrs[9]);
        this.dateTime = Long.parseLong(attrs[2]);

    }
}
