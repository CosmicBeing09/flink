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

package org.apache.flink.api.java.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataStream;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Resulting {@link DataStream} of bulk iterations.
 *
 * @param <T>
 */
@Internal
public class BulkIterationResultSet<T> extends DataStream<T> {

    private final IterativeDataSet<T> iterationHead;

    private final DataStream<T> nextPartialSolution;

    private final DataStream<?> terminationCriterion;

    BulkIterationResultSet(
            ExecutionEnvironment context,
            TypeInformation<T> type,
            IterativeDataSet<T> iterationHead,
            DataStream<T> nextPartialSolution) {
        this(context, type, iterationHead, nextPartialSolution, null);
    }

    BulkIterationResultSet(
            ExecutionEnvironment context,
            TypeInformation<T> type,
            IterativeDataSet<T> iterationHead,
            DataStream<T> nextPartialSolution,
            DataStream<?> terminationCriterion) {
        super(context, type);
        this.iterationHead = iterationHead;
        this.nextPartialSolution = nextPartialSolution;
        this.terminationCriterion = terminationCriterion;
    }

    public IterativeDataSet<T> getIterationHead() {
        return iterationHead;
    }

    public DataStream<T> getNextPartialSolution() {
        return nextPartialSolution;
    }

    public DataStream<?> getTerminationCriterion() {
        return terminationCriterion;
    }
}
