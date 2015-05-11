/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.adapter.hbase;

import java.io.IOException;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;


public class HBaseSchema extends AbstractSchema {

    private final HTable table;

    public HBaseSchema(String tableName,
        String alias,
        String[] columnFamilies,
        Configuration conf) {
        try {
            this.table = new HTable(conf, tableName);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }

    @Override
    protected Map<String, Table> getTableMap() {
        return super.getTableMap();
    }
}
