package com.oycm.jobhandler;

import com.alibaba.fastjson2.JSON;
import com.oycm.autoconfigure.SparkJobProperties;
import com.oycm.enums.SparkJobType;
import com.oycm.jobconfig.spark.SimpleSqlETLParam;
import com.oycm.util.FileUtil;
import com.xxl.job.core.context.XxlJobContext;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.IJobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Simple Spark ETL xxl-job 执行器
 * @author ouyangcm
 * create 2025/9/19 16:00
 */
public class SimpleSparkJobHandler extends IJobHandler {

    private static final Logger log = LoggerFactory.getLogger(SimpleSparkJobHandler.class);


    private final SparkJobProperties sparkJobProperties;


    public SimpleSparkJobHandler(SparkJobProperties sparkJobProperties) {
        this.sparkJobProperties = sparkJobProperties;
    }

    @Override
    public void execute() throws Exception {
        // xxl-job 任务上下文
        XxlJobContext jobContext = XxlJobContext.getXxlJobContext();

        long jobId = jobContext.getJobId();

        log.info("job is: [{}], params is: [{}]", jobId, jobContext.getJobParam());
        SimpleSqlETLParam jobParam = JSON.parseObject(jobContext.getJobParam(), SimpleSqlETLParam.class);
        jobParam.setJobId(jobId);

        SparkJobType sparkJobType = SparkJobType.getByName(jobParam.getJobType());
        if (sparkJobType == null) {
            XxlJobHelper.handleFail("jobType [" + jobParam.getJobType() + "] not support!");
            return;
        }

        // 任务参数校验
        String result = jobParam.paramCheck();
        if (result.length() > 0) {
            log.info("job param check fail: {}", result);
            XxlJobHelper.handleFail(result);
            return;
        }

        jobParam.setSparkJobType(sparkJobType);

        exec(jobParam, FileUtil.writeSparkJobJson(jobId, JSON.toJSONString(jobParam)));
    }

    public void init() throws Exception {
        log.info(SimpleSparkJobHandler.class.getSimpleName() + " init");
    }

    public void destroy() throws Exception {
        log.info(SimpleSparkJobHandler.class.getSimpleName() + " destroy");
    }


    private void exec(SimpleSqlETLParam jobParam, String jobFile) {
        try {
            String jars = FileUtil.getDatasourceJar(jobParam.getDatasourceSet());

            String command = String.join(" ",
                    sparkJobProperties.getRelativePath() + "/bin/spark-submit",
                    "--class " + jobParam.getSparkJobType().getClazz(),
                    "--master " + sparkJobProperties.getMaster(),
                    sparkJobProperties.getDefaultJobConf(),
                    "--jars " + jars,
                    jobParam.getSparkJobType().getJarPath(),
                    "" + jobParam.getJobId(),
                    jobFile);

            // linux 执行命令
            String[] cmd;
            if (System.getProperty("os.name").contains("Windows")) {
                cmd = new String[]{"cmd", "/c", command};
            } else {
                cmd = new String[]{"bash", "-c", command};
            }
            log.info(command);
            XxlJobHelper.log("spark job submit {}", command);

            Process process;
            try {
                process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
                // todo 考虑异步读取日志
                try (InputStream in = process.getInputStream()) {
                    boolean isSucceed = FileUtil.xxlLog(in, XxlJobContext.getXxlJobContext());
                    if (!isSucceed) {
                        XxlJobHelper.handleFail("read spark log error!");
                    }
                }
            } catch (IOException e) {
                log.error("spark submit error: ", e);
                XxlJobHelper.handleFail(e.getMessage());
            }

        } catch (Exception e) {
            log.error("exec error", e);
            XxlJobHelper.handleFail(e.getMessage());
        }
    }
}
