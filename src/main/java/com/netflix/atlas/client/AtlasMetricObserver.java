/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.atlas.client;

import com.netflix.servo.Metric;
import com.netflix.servo.tag.TagList;
import rx.Observable;

class AtlasMetricObserver extends BaseAtlasMetricObserver {
    AtlasMetricObserver(PluginConfig config) {
        super(config, config, 0);
    }

    @Override
    Observable<Integer> getSenderObservable(TagList tags, Metric[] batch) {
        JsonPayload payload = new UpdateRequest(tags, batch, batch.length, getStepMs());
        return RxHttp.postSmile(getPluginConfig().getPublishUri(), payload)
                .map(withBookkeeping(batch.length));
    }
}
