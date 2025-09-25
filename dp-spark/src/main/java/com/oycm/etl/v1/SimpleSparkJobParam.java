package com.oycm.etl.v1;

import com.oycm.enums.JdbcDatasourceType;
import com.oycm.spark.jdbc.CusOption;
import com.oycm.spark.jdbc.DataEtlConfig;
import com.oycm.spark.jdbc.ReadJdbcConfig;
import com.oycm.spark.jdbc.WriteJdbcConfig;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.execution.datasources.jdbc.CusSaveMode;

import java.util.List;

/**
 * @author ouyangcm
 * create 2025/9/24 13:07
 */
@Data
public class SimpleSparkJobParam {

    /**
     * jdbc 读配置
     */
    private List<ReadJdbcConfig> readJdbcConfigs;

    /**
     * 读数据转换配置
     */
    private DataEtlConfig dataEtlConfig;

    /**
     * jdbc 写配置
     */
    private WriteJdbcConfig writeJdbcConfig;

    public String check() {
        StringBuilder result = new StringBuilder();

        // 读配置校验
        if (readJdbcConfigs == null || readJdbcConfigs.isEmpty()) {
            result.append("readJdbcConfigs is empty; ");
        } else {
            for (int i = 0; i < readJdbcConfigs.size(); i++) {
                ReadJdbcConfig readJdbcConfig = readJdbcConfigs.get(i);
                if (StringUtils.isBlank(readJdbcConfig.getUrl())) {
                    result.append("readJdbcConfigs[").append(i).append("] url is empty; ");
                }else {
                    JdbcDatasourceType datasourceType = JdbcDatasourceType.getByJdbcUrl(readJdbcConfig.getUrl());
                    if (datasourceType == null) {
                        result.append("readJdbcConfigs[").append(i).append("] database not support; ");
                    } else {
                        readJdbcConfig.setDriver(datasourceType.getDriver());
                        // SecureConnectionProvider.canHandle 逻辑匹配才能使用对应的连接
                        // keytab 和 principal 配置才使用; 都不配置, 匹配 basic
                        if (StringUtils.isNotBlank(readJdbcConfig.getKeytab()) && StringUtils.isNotBlank(readJdbcConfig.getPrincipal())) {
                            if (JdbcDatasourceType.oracle == datasourceType) {
                                readJdbcConfig.setConnectionProvider(CusOption.CONNECTION_PROVIDER_ORACLE);
                            } else if (JdbcDatasourceType.postgresql == datasourceType) {
                                readJdbcConfig.setConnectionProvider(CusOption.CONNECTION_PROVIDER_POSTGRES);
                            }
                        } else {
                            readJdbcConfig.setConnectionProvider(CusOption.CONNECTION_PROVIDER_BASIC);
                        }

                    }
                }

                if (StringUtils.isBlank(readJdbcConfig.getUser())) {
                    result.append("readJdbcConfigs[").append(i).append("] user is empty; ");
                }

                if (StringUtils.isBlank(readJdbcConfig.getPassword())) {
                    result.append("readJdbcConfigs[").append(i).append("] password is empty; ");
                }

                if (StringUtils.isBlank(readJdbcConfig.getQuery()) && StringUtils.isBlank(readJdbcConfig.getDbTable())) {
                    result.append("readJdbcConfigs[").append(i).append("] query and dbTable is empty; ");
                }

                if (StringUtils.isNotBlank(readJdbcConfig.getPartitionColumn())) {
                    if (StringUtils.isBlank(readJdbcConfig.getLowerBound()) || StringUtils.isBlank(readJdbcConfig.getUpperBound())) {
                        result.append("readJdbcConfigs[").append(i).append("] lowerBound or upperBound is empty; ");
                    }
                }
            }
        }

        // dataEtlConfig 不配置, 使用 readJdbcConfigs[0] 写入
        if (dataEtlConfig != null && StringUtils.isBlank(dataEtlConfig.getSql())) {
            result.append("dataEtlConfig.sql is empty; ");
        }

        if (writeJdbcConfig == null) {
            result.append("writeJdbcConfig is empty; ");
        }else {
            if (StringUtils.isBlank(writeJdbcConfig.getUrl())) {
                result.append("writeJdbcConfig url is empty; ");
            } else {
                JdbcDatasourceType datasourceType = JdbcDatasourceType.getByJdbcUrl(writeJdbcConfig.getUrl());
                if (datasourceType == null) {
                    result.append("writeJdbcConfig database not support; ");
                } else {
                    writeJdbcConfig.setDriver(datasourceType.getDriver());
                    if (StringUtils.isNotBlank(writeJdbcConfig.getKeytab()) && StringUtils.isNotBlank(writeJdbcConfig.getPrincipal())) {
                        if (JdbcDatasourceType.oracle == datasourceType) {
                            writeJdbcConfig.setConnectionProvider(CusOption.CONNECTION_PROVIDER_ORACLE);
                        } else if (JdbcDatasourceType.postgresql == datasourceType) {
                            writeJdbcConfig.setConnectionProvider(CusOption.CONNECTION_PROVIDER_POSTGRES);
                        }
                    } else {
                        writeJdbcConfig.setConnectionProvider(CusOption.CONNECTION_PROVIDER_BASIC);
                    }
                }
            }

            if (StringUtils.isBlank(writeJdbcConfig.getUser())) {
                result.append("writeJdbcConfig user is empty; ");
            }

            if (StringUtils.isBlank(writeJdbcConfig.getPassword())) {
                result.append("writeJdbcConfig password is empty; ");
            }

            if (StringUtils.isBlank(writeJdbcConfig.getQuery()) && StringUtils.isBlank(writeJdbcConfig.getDbTable())) {
                result.append("writeJdbcConfig query and dbTable is empty; ");
            }

            if (CusSaveMode.getByName(writeJdbcConfig.getSaveMode()) == null) {
                result.append("writeJdbcConfig saveMode not support; ");
            }

            if (CusSaveMode.Update.name().equalsIgnoreCase(writeJdbcConfig.getSaveMode())) {
                writeJdbcConfig.setUseCusJdbc(true);
                if (StringUtils.isBlank(writeJdbcConfig.getUpdateUniqueKey())) {
                    result.append("writeJdbcConfig config saveMode is Update, updateUniqueKey is empty; ");
                }

            }


        }

        return result.toString();
    }

}
