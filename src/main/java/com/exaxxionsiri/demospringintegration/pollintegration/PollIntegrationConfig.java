package com.exaxxionsiri.demospringintegration.pollintegration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.handler.LoggingHandler;

import java.io.File;

@Slf4j
@Configuration
@EnableIntegration
public class PollIntegrationConfig {

    @Bean
    public FileMessageToJobRequest fileMessageToJobRequest(Job personJob) {
        FileMessageToJobRequest fileMessageToJobRequest = new FileMessageToJobRequest();
        fileMessageToJobRequest.setFileParameterName("input.file.name");
        fileMessageToJobRequest.setJob(personJob);
        return fileMessageToJobRequest;
    }

    @Bean
    public JobLaunchingGateway jobLaunchingGateway(JobRepository jobRepository) {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SyncTaskExecutor());

        return new JobLaunchingGateway(jobLauncher);
    }

    @Bean
    public IntegrationFlow integrationFlow(JobLaunchingGateway jobLaunchingGateway) {
        File directory = new File("L:\\work\\workspace\\demospringintegration\\src\\main\\resources");
//        File inputDir = new File("file://"+directory.getAbsolutePath());
        log.info(directory.getAbsolutePath());

        if(!directory.exists()){
            throw new IllegalArgumentException("directory does not exist");
        }

        return IntegrationFlow.from(Files.inboundAdapter(directory).
                                filter(new SimplePatternFileListFilter("*.csv")),
                        c -> c.poller(Pollers.fixedRate(5000).maxMessagesPerPoll(1))).
                transform(fileMessageToJobRequest(null)).
                handle(jobLaunchingGateway)
                .channel("nullChannel")
                .log(LoggingHandler.Level.WARN, "headers.id + ': ' + payload")
                .get();
    }

}
