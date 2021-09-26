package com.nitin.config;

import java.util.function.Function;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.nitin.common.CommunicationMode;
import com.nitin.model.Notification;

@Configuration
public class Streamprocesor {
	
    @Bean
    public Function<KStream<String, Notification>,KStream<String, Notification>> process() {
    	return kstream -> 
    		kstream.filter((key,notification) -> 
    		notification.getCommunicationMode() == CommunicationMode.EMAIL);
    }
}
