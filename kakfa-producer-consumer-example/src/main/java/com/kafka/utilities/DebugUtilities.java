package com.kafka.utilities;

import com.kafka.constants.IKafkaConstants;

public class DebugUtilities {

    public void printDebug(String toPrint)
    {
        if (IKafkaConstants.DEBUG_MODE)
        {
            System.out.println(toPrint);
        }
    }

}
