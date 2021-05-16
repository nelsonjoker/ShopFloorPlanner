/*
 * Copyright 2015 JBoss Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.joker.planner.solver;

import com.joker.planner.domain.Operation;
import com.joker.planner.domain.Resource;

public abstract class ResourceCapacityTracker {

    protected Resource resource;
    
    public ResourceCapacityTracker(Resource resource) {
        this.resource = resource;
    }

    public abstract void insert(int resourceRequirement, Operation op);

    public abstract void retract(int resourceRequirement, Operation op);

    public abstract long getHardScore();

}
