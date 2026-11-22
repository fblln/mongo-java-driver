/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.internal

import org.bson.codecs.MinKeyCodec
import org.bson.types.MinKey
import spock.lang.Specification

class CodecCacheSpecification extends Specification {

    def 'should return the cached codec if a codec for the class exists'() {
        when:
        def codec = new MinKeyCodec()
        def cache = new CodecCache()
        cache.putIfAbsent(MinKey, null, codec)

        then:
        cache.get(MinKey, null).is(codec)
        cache.get(MinKey, []) == null
    }

    def 'should return empty if codec for class does not exist'() {
        when:
        def cache = new CodecCache()

        then:
        cache.get(MinKey, null) == null
    }

    def 'should return the cached codec if a codec for the parameterized class exists'() {
        when:
        def codec = new MinKeyCodec()
        def cache = new CodecCache()
        cache.putIfAbsent(List, [Integer], codec)

        then:
        cache.get(List, [Integer]).is(codec)
        cache.get(List, null) == null
        cache.get(List, []) == null
        cache.get(List, [String]) == null
    }

    def 'should return empty if codec for the parameterized class does not exist'() {
        when:
        def cache = new CodecCache()

        then:
        cache.get(List, [Integer]) == null
    }
}
