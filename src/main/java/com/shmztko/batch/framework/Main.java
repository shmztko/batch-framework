package com.shmztko.batch.framework;

import com.shmztko.batch.framework.executable.Executable;
import org.easybatch.core.job.Job;
import org.easybatch.core.job.JobBuilder;
import org.kohsuke.args4j.Option;

import java.io.File;

public class Main extends Executable {

    @Option(required = true, name = "-f", usage = "Process target directory.")
    private String targetDirectoryPath;

    @Override
    protected Job createJob() throws Exception {
        System.out.println(targetDirectoryPath);
        return JobBuilder.aNewJob()
                .reader(new FileLineReader(new File(targetDirectoryPath)))
                .writer((record) -> {
                    System.out.println(record.getPayload());
                    return record;
                }).build();
    }

    public class Hoge {
        public String value1;
        public String value2;
        public Hoge(String value1, String value2) {
            this.value1 = value1;
            this.value2 = value2;
        }

        @Override
        public String toString() {
            return "Value1 : " + value1 + "; Value2 : " + value2;
        }
    }

    public static void main(String[] args) {
        try {
            new Main().execute(args);
            System.exit(0);
        } catch (Exception e) {
            System.exit(1);
        }
    }
}
