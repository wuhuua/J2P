package com.unloadhome.Basic;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BasicTest {
    @Test
    public void test_range_should_get_iterator_when_execute(){
        int num=0;
        for(Number i:new Range(5)){
            assertEquals(num,i);
            num++;
        }
    }
}
