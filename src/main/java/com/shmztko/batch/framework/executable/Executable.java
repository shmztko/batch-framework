package com.shmztko.batch.framework.executable;

import org.easybatch.core.job.Job;
import org.easybatch.core.job.JobReport;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.LocalDateTime;

public abstract class Executable {

    private Logger logger;

    private String processId;

    private CmdLineParser parser;

    public Executable() {
        this.logger = LoggerFactory.getLogger(getClass());
        this.processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

        this.parser = new CmdLineParser(this);
    }

    public void execute(String[] args) throws Exception {
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            getLogger().warn("Failed to parse command line arguments.", e);
            parser.printUsage(System.out);
            throw e;
        }

        LocalDateTime start = LocalDateTime.now();
        getLogger().info("Process[ID:{}] started at {}", processId, start);
        try {
            beforeProcess();

            process();

        } catch (Exception e) {
            getLogger().error("Failed to execute command.", e);
            handleException(e);
            throw e;

        } finally {
            afterProcess();
            LocalDateTime finish = LocalDateTime.now();
            Duration duration = Duration.between(start, finish);
            getLogger().info("Process[ID:{}] finished at {}. Duration: {} [sec]", processId, finish, duration.getSeconds());
        }
    }

    protected void beforeProcess() throws Exception {
        // NOOP
    }

    protected void process() throws Exception {
        Job job = createJob();
        getLogger().info("Running job {}", job);
        JobReport report = job.call();
        getLogger().info("Job completed.");
        getLogger().info("Job report. {}", report);
    }

    protected abstract Job createJob() throws Exception;

    protected void afterProcess() {
        // NOOP
    }

    protected void handleException(Exception e) {
        // NOOP
    }

    protected Logger getLogger() {
        return this.logger;
    }
}
