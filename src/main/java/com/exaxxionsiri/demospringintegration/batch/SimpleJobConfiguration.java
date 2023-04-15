package com.exaxxionsiri.demospringintegration.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.Resource;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

@Slf4j
@Configuration
//@EnableBatchIntegration
public class SimpleJobConfiguration {



//    @Bean
//    @StepScope
//    public ItemReader sampleReader(@Value("#{jobParameters[input.file.name]}") String resource) {
//        FlatFileItemReader flatFileItemReader = new FlatFileItemReader();
//        flatFileItemReader.setResource(new FileSystemResource(resource));
//        return flatFileItemReader;
//    }

    @Bean
    @StepScope
    public ItemStreamReader<Person> reader(@Value("#{jobParameters['input.file.name']}") String  resource) throws IOException {

        log.info(resource);

        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new FileSystemResource(resource))
                .delimited().delimiter(";")
                .names("firstName", "lastName")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(Person.class);
                }})
                .build();
    }

    @Bean
    @Lazy
    public Job personJob(JobRepository jobRepository, Step step1, Step taskletStep) {
        return new JobBuilder("importUserJob", jobRepository)
                .incrementer(new RunIdIncrementer())
//                .listener(listener)
                .flow(step1)
                .next(taskletStep)
                .end()
                .build();
    }

    @Lazy
    @Bean
    public Step step1(JobRepository jobRepository,
                      PlatformTransactionManager transactionManager) throws IOException {
        return new StepBuilder("step1", jobRepository)
                .<Person, Person> chunk(10, transactionManager)
                .reader(reader(null))
                .processor(getPersonItemProcessor())
                .writer(getPersonItemWriter())
                .build();
    }

    @Bean
    @StepScope
    public Step taskletStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager){
        return new StepBuilder("tasklet", jobRepository).tasklet( tasklet(null),platformTransactionManager).build();
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{jobParameters['input.file.name']}") String  resource){
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

                FileSystemResource fileSystemResource = new FileSystemResource(resource);
                log.info(fileSystemResource.toString());


                return null;
            }
        };
    }

    @Bean
    public PersonItemProcessor getPersonItemProcessor(){
        return new PersonItemProcessor();
    }

    @Bean
    public PersonItemWriter getPersonItemWriter(){
        return new PersonItemWriter();
    }

}
