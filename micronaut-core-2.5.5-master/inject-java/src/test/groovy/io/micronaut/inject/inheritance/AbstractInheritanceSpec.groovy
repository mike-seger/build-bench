/*
 * Copyright 2017-2019 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micronaut.inject.inheritance

import io.micronaut.context.BeanContext
import io.micronaut.context.DefaultBeanContext
import spock.lang.Specification

class AbstractInheritanceSpec extends Specification {

    void "test values are injected for abstract parent class"() {
        given:
        BeanContext context  = new DefaultBeanContext()
        context.start()

        when:"A bean is retrieved that has abstract inherited values"
        B b = context.getBean(B)

        then:"The values are injected"
        b.a != null
        b.another != null
        b.a.is(b.another)
        b.packagePrivate != null
        b.packagePrivate.is(b.another)
    }
}
