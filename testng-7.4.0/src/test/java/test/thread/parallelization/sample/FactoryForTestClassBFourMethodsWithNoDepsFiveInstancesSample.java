package test.thread.parallelization.sample;

import org.testng.annotations.Factory;

import java.util.ArrayList;
import java.util.List;

public class FactoryForTestClassBFourMethodsWithNoDepsFiveInstancesSample {
    @Factory
    public Object[] init() {
        List<Object> instances = new ArrayList<>();

        try {
            instances.add(TestClassBFourMethodsWithNoDepsSample.class.newInstance());
            instances.add(TestClassBFourMethodsWithNoDepsSample.class.newInstance());
            instances.add(TestClassBFourMethodsWithNoDepsSample.class.newInstance());
            instances.add(TestClassBFourMethodsWithNoDepsSample.class.newInstance());
            instances.add(TestClassBFourMethodsWithNoDepsSample.class.newInstance());
        } catch (InstantiationException e) {
            throw new RuntimeException(
                    "Could not instantiate an instance of TestClassBFourMethodsWithNoDepsSample because it is " +
                            "abstract or for some other reason", e
            );
        } catch (IllegalAccessException e) {
            throw new RuntimeException(
                    "Could not instantiate an instance of TestClassBFourMethodsWithNoDepsSample " +
                            "FactoryForTestClassBFourMethodsWithNoDepsFiveInstancesSample does not have access to its " +
                            "class definition", e
            );
        }

        return instances.toArray();
    }
}
