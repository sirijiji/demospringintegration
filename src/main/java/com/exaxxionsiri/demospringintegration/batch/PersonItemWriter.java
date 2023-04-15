package com.exaxxionsiri.demospringintegration.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

@Slf4j
public class PersonItemWriter implements ItemWriter<Person> {
    @Override
    public void write(Chunk<? extends Person> chunk) throws Exception {

        log.info("writer {}", chunk);
    }
}
