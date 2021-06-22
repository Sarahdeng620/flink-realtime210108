package com.atguigu.app.flinkBase;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink10_JdbcSink {
    public static void main(String[] args) throws Exception {

        //TODO 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.从端口读取数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new WaterSensor(fields[0],
                            Long.parseLong(fields[1]),
                            Double.parseDouble(fields[2]));
                });
        //TODO 3.将数据写入MySQL
        streamOperator.addSink(JdbcSink.sink(
                //TODO 3.1 sql语句 预留出占位符
                "insert into water_sensor values(?,?,?)",
                //TODO 3.2 预发送语句，将 流数据 写入 预发送语句的占位符
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                        preparedStatement.setObject(1, waterSensor.getId());
                        preparedStatement.setObject(2, waterSensor.getTs());
                        preparedStatement.setObject(3, waterSensor.getVc());

                    }
                },
                //TODO 3.3 设置mysql 写库操作，批处理的设置参数
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .build(),
                //TODO 3.4 sql数据库连接参数
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUrl("jdbc:mysql://hadoop102:3306/flink-210108?useSSL=false")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));
        //TODO 4.启动
        env.execute();
    }
}
