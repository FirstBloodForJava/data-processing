**dp-xxl-job-exeutor.jar 启动 shell 脚本**

~~~bash
#!/bin/bash
APP_NAME=dp-xxl-job-executor
if [ -n "$JAVA_HOME" ];then
  JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
else
  echo "no JAVA_HOME PATH"
  exit 0
fi
# -Xms: 堆内存初始化大小; -Xmx: 堆内存最大大小; -XX:NewRatio: 老年代和新生代的比例
MEM_OPTS="-Xms512m -Xmx512m -XX:NewRatio=1"

if [[ "$JAVA_VERSION" < "1.8" ]]; then
  MEM_OPTS="$MEM_OPTS -XX:PermSize=256m -XX:MaxPermSize=256m"
else
  MEM_OPTS="$MEM_OPTS -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=256m"
fi
# 启动时预申请内存
MEM_OPTS="$MEM_OPTS -XX:+AlwaysPreTouch"
# 线程栈分配大小。
MEM_OPTS="$MEM_OPTS -Xss256k"
# 回收比例。Grafana90报警。预先设置为80回收。并使用CMS. 看业务情况可动态调整。包括上面的堆内存的分配。
GC_OPTS="-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -XX:+UseCMSInitiatingOccupancyOnly"
# 自动开箱缓存
OPTIMIZE_OPTS="-XX:AutoBoxCacheMax=20000"
SHOOTING_OPTS="-XX:-OmitStackTraceInFastThrow -XX:ErrorFile=${LOGDIR}/hs_err_%p.log"

JAVA_OPTS="$MEM_OPTS $GC_OPTS $OPTIMIZE_OPTS $SHOOTING_OPTS"

JAR_NAME=${APP_NAME}".jar"
PID=$(ps aux | grep ${JAR_NAME} | grep -v grep | awk '{print $2}' )

check_if_process_is_running(){
 if [ "$PID" = "" ]; then
   return 1
 fi
 ps -p $PID | grep "java"
 return $?
}

GREEN='\033[32m'
# 重置颜色
RESET='\033[0m'

case "$1" in
  status)
    if check_if_process_is_running
    then
      echo -e "${GREEN} $APP_NAME is running ${RESET}"
    else
      echo -e "${GREEN} $APP_NAME not running ${RESET}"
    fi
    ;;
  stop)
    if ! check_if_process_is_running
    then
      echo  -e "${GREEN} $APP_NAME  already stopped ${RESET}"
      exit 0
    fi
	kill -3 $PID
    kill $PID
    echo -e "${GREEN} Waiting for process to stop ${RESET}"
    NOT_KILLED=1
    for i in {1..20}; do
      if check_if_process_is_running
      then
        echo -ne "${GREEN} . ${RESET}"
        sleep 1
      else
        NOT_KILLED=0
      fi
    done
    echo
    if [ $NOT_KILLED = 1 ]
    then
      echo -e "${GREEN} Cannot kill process ${RESET}"
      exit 1
    fi
    echo  -e "${GREEN} $APP_NAME already stopped ${RESET}"
    ;;
  start)
    if [ "$PID" != "" ] && check_if_process_is_running
    then
      echo -e "${GREEN} $APP_NAME already running ${RESET}"
      exit 1
    fi
   nohup java -jar -server $JAVA_OPTS $JAR_NAME > output 2>&1 &
   echo -ne "${GREEN} Starting ${RESET}"
    for i in {1..3}; do
        echo -ne "${GREEN}.${RESET}"
        sleep 1
    done
    if check_if_process_is_running
     then
       echo  -e "${GREEN} $APP_NAME fail ${RESET}"
    else
       echo  -e "${GREEN} $APP_NAME started ${RESET}"
    fi
    ;;
  restart)
    $0 stop
    if [ $? = 1 ]
    then
      exit 1
    fi
    $0 start
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
esac

exit 0

~~~



**xxl-job 参数 json 格式，xxl_job_info 字段 executor_param 需要扩容**

~~~json
{
    "jobType": "SimpleSqlETL",
    "readJdbcConfigs": [
        {
            "fetchSize": 1000,
            "name": "tempName",
            "query": "",
            "url": "jdbc:oracle:thin:@ip:port/service",
            "user": "user",
            "password": "password",
            "dbTable": "table"
        }
    ],
    "dataEtlConfig": {
        "useUpdateCol": true,
        "sql": ""
    },
    "writeJdbcConfig": {
        "batchSize": "1000",
        "saveMode": "Update",
        "url": "jdbc:oracle:thin:@ip:port/service",
        "user": "user",
        "password": "password",
        "dbTable": "table",
        "updateUniqueKey": "ID"
    }
}
~~~

