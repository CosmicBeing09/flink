/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Core result of {@link SlotAllocator#determineParallelism(JobInformation, Collection)} among with
 * {@link org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment
 * slotAssignments}, describing the parallelism each vertex could be scheduled with.
 */
public class VertexParallelism {
    private final Map<JobVertexID, Integer> vertexParallelismMap;

    public VertexParallelism(Map<JobVertexID, Integer> vertexParallelismMap) {
        this.vertexParallelismMap = vertexParallelismMap;
    }

    public int getParallelism(JobVertexID jobVertexId) {
        checkArgument(
                vertexParallelismMap.containsKey(jobVertexId), "Unknown vertex: " + jobVertexId);
        return vertexParallelismMap.get(jobVertexId);
    }

    public Optional<Integer> getParallelismOptional(JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexParallelismMap.get(jobVertexId));
    }

    public Set<JobVertexID> getVertices() {
        return Collections.unmodifiableSet(vertexParallelismMap.keySet());
    }

    @Override
    public String toString() {
        return "VertexParallelism: " + vertexParallelismMap;
    }

    public static VertexParallelism empty() {
        return new VertexParallelism(Collections.emptyMap());
    }
}
