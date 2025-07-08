package com.example.kafka.stream;

import com.example.kafka.dto.MyDTO;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class StreamService {

    @Bean
    public JsonSerde<MyDTO> myDTOEventSerde() {
        JsonSerde<MyDTO> serde = new JsonSerde<>(MyDTO.class);
        Map<String, Object> config = new HashMap<>();
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MyDTO.class.getName());
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        serde.configure(config, false);
        return serde;
    }

    @Bean
    public KStream<String, MyDTO> kStream(StreamsBuilder streamsBuilder) {
        JsonSerde<MyDTO> myDTOEventSerde = myDTOEventSerde();
        KStream<String, MyDTO> stream = streamsBuilder
                .stream("topic-in", Consumed.with(Serdes.String(), myDTOEventSerde));
        stream.mapValues(data -> {
                    data.setName(data.getName() + " - add process by stream");
                    return data;
                })
                .to("topic-out");
        return stream;
    }
}
