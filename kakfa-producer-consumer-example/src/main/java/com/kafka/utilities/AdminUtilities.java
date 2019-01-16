package com.kafka.utilities;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminUtilities {

    private Properties properties;
    private AdminClient adminClient;

    public AdminUtilities() {
        properties = new Properties();
        try {
            properties.load(new FileReader(new File("C:\\Users\\EDI2\\Desktop\\kafka-master\\properties\\kafka.properties")));
        } catch (
                IOException e) {
            e.printStackTrace();
        }

    }

    public void createTopic(String name, int replicationFactor) {
        AdminClient adminClient = AdminClient.create(properties);

        NewTopic newTopic = new NewTopic(name, 1, (short) replicationFactor); //new NewTopic(topicName, numPartitions, replicationFactor);

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);

        adminClient.createTopics(newTopics);

        adminClient.close();
    }


    public void listTopics() {
        AdminClient adminClient = AdminClient.create(properties);
        adminClient.listTopics().names();
        try {
            for (String name : adminClient.listTopics().names().get()) {
                System.out.println(name);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}