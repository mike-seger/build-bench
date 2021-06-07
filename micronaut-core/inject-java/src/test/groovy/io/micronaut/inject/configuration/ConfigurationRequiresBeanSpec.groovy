package io.micronaut.inject.configuration

import io.micronaut.context.BeanContext
import io.micronaut.context.DefaultBeanContext
import io.micronaut.inject.configuration.beans.B
import io.micronaut.inject.configuration.beans.disabled.D
import spock.lang.Specification

class ConfigurationRequiresBeanSpec extends Specification {

    void "test a configuration that requires a bean"() {
        BeanContext context = new DefaultBeanContext().start()

        expect:
        context.containsBean(B) //because A is a bean
        !context.containsBean(D) //because C is not a bean. also requires A
    }
}
