package com.atguigu.app.flinkBase;

import com.atguigu.bean.IdCount;
import com.atguigu.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class Flink09_02 {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.读取端口数据 提取时间戳生成WaterMark
        SingleOutputStreamOperator<String> socketTextStream = env.socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return Long.parseLong(split[1]) * 1000L;
                    }
                }));

        //TODO 3.将每行数据转换为JavaBean对象,提取时间戳生成WaterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(line -> {
            String[] fields = line.split(",");
            return new WaterSensor(fields[0],
                    Long.parseLong(fields[1]),
                    Double.parseDouble(fields[2]));
        });

        waterSensorDS.print("waterSensorDS>>>>>");


        //TODO 4.按照传感器ID分组（为社么分组？keyBy后好开窗进入一个窗口计算
        // 是将数据变成（hello,1）的结构，并分组  放在一个并行度 供后面处理）
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = waterSensorDS
                .map(new MapFunction<WaterSensor, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(WaterSensor value) throws Exception {
                        return new Tuple2<>(value.getId(), 1);
                    }
                })
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        //TODO 5.开窗 （什么样的窗口？）
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .allowedLateness(Time.seconds(5));

        //TODO 6.聚合,计算WordCount 并 添加窗口的信息（窗口的结束时间的时间戳）
        SingleOutputStreamOperator<IdCount> aggregateDS = windowedStream.aggregate(new AggFunc(), new IdCountWindowFunc());
        aggregateDS.print("aggregateDS>>>>>>>>");

        //TODO 7.按照窗口信息重新分组 即将（相同窗口结束时间） 的数据放到一起去 即将一个窗口的数据放到一起
        KeyedStream<IdCount, Long> tsKeyedStream = aggregateDS.keyBy(IdCount::getTs);

        //TODO 8.ProcessFunction利用状态编程 + 定时器，完成最终的排序任务
        SingleOutputStreamOperator<String> result = tsKeyedStream.process(new TopNCountFunc(3));

        //TODO 9.打印结果
        result.print();

        //TODO 10.启动
        env.execute();

    }

    public static class AggFunc implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        //TODO 6.1 数据来一条聚合一条，利用增量聚合的窗口函数
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    //TODO 6.2 窗口函数之增量聚合函数 一条一条聚合  + 聚合完成后可以获得整个窗口的窗口信息
    public static class IdCountWindowFunc implements WindowFunction<Integer, IdCount, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input, Collector<IdCount> out) throws Exception {

            //TODO 6.2.1取出Count数据
            Integer ct = input.iterator().next();

            //TODO 6.2.2 获取窗口信息 中的窗口结束的时间
            long windowEnd = window.getEnd();

            //TODO 6.2.3 写出数据
            out.collect(new IdCount(windowEnd, key, ct));

        }
    }

    //TODO 9.1 将一个窗口结束后的数据进行处理，得到TopN
    public static class TopNCountFunc extends KeyedProcessFunction<Long, IdCount, String> {

        //TODO 9.1.1 定义属性
        private Integer topSize;

        public TopNCountFunc(Integer topSize) {
            this.topSize = topSize;
        }

        //TODO 9.1.2 定义状态  （为什么用map不用list? 因为map 的key 不重复，可以覆写）
        private MapState<String, IdCount> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, IdCount> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, IdCount.class);

            mapStateDescriptor.enableTimeToLive(new StateTtlConfig.Builder(org.apache.flink.api.common.time.Time.seconds(10))
                    .setUpdateType(StateTtlConfig.UpdateType.Disabled)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .build());

            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void processElement(IdCount value, Context ctx, Collector<String> out) throws Exception {
            //TODO 9.1.3 将数据加入状态
            mapState.put(value.getId(), value);

            //TODO 9.1.4 注册定时器 1,用于触发当数据收集完成时,排序输出
            // 主要是 等等 同一个窗口结束时间的数据（因存在传输延迟  可设置为 100Lms）
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(value.getTs() + 100L);

            //TODO 9.1.5 注册定时器 2 用于触发状态清除
            // （每结束处于相同窗口时间，需清楚这个窗口的状态变量什么时候清除？）
            // （注意：5000L 为窗口的延迟时间allowedLateness(Time.seconds(5))） 5s
            timerService.registerEventTimeTimer(value.getTs() + 5000L + 100L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            if (timestamp == ctx.getCurrentKey() + 5000L + 100L) {
                //TODO 9.1.5 清空状态
                mapState.clear();
                return;
            }

            //TODO 9.1.4.1 取出状态中的数据 并将Lists.newArrayList(iterator) 转换成  ArrayList<IdCount>
            Iterator<IdCount> iterator = mapState.values().iterator();
            ArrayList<IdCount> idCounts = Lists.newArrayList(iterator);

            //TODO 9.1.4.2 排序
            idCounts.sort(new Comparator<IdCount>() {
                @Override
                public int compare(IdCount o1, IdCount o2) {
                    if (o1.getCt() > o2.getCt()) {
                        return -1;
                    } else if (o1.getCt() < o2.getCt()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            //TODO 9.1.4.3 输出
            StringBuilder sb = new StringBuilder("===================\n");
            for (int i = 0; i < Math.min(idCounts.size(), topSize); i++) {

                //取出数据
                IdCount idCount = idCounts.get(i);
                sb
                        .append("Top").append(i + 1)
                        .append(",ID:").append(idCount.getId())
                        .append(",CT:").append(idCount.getCt())
                        .append("\n");
            }
            sb.append("===================\n");
            out.collect(sb.toString());

            //TODO 1.获取执行环境

            //TODO 2.读取端口数据 提取时间戳生成WaterMark

            //TODO 3.将每行数据转换为JavaBean对象,提取时间戳生成WaterMark

            //TODO 4.按照传感器ID分组（为社么分组？keyBy后好开窗进入一个窗口计算

            //TODO 5.开窗 （什么样的窗口？）

            //TODO 6.聚合,计算WordCount 并 添加窗口的信息（窗口的结束时间的时间戳）

            //TODO 7.按照窗口信息重新分组 即将（相同窗口结束时间） 的数据放到一起去 即将一个窗口的数据放到一起

            //TODO 8.ProcessFunction利用状态编程 + 定时器，完成最终的排序任务

            //TODO 9.打印结果

            //TODO 10.启动
            //TODO 6.1 数据来一条聚合一条，利用增量聚合的窗口函数

            //TODO 6.2 窗口函数之增量聚合函数 一条一条聚合  + 聚合完成后可以获得整个窗口的窗口信息
            //TODO 6.2.1取出Count数据
            //TODO 6.2.2 获取窗口信息 中的窗口结束的时间
            //TODO 6.2.3 写出数据
            //TODO 9.1 将一个窗口结束后的数据进行处理，得到TopN
            //TODO 9.1.1 定义属性
            //TODO 9.1.2 定义状态  （为什么用map不用list? 因为map 的key 不重复，可以覆写）
            //TODO 9.1.3 将数据加入状态
            //TODO 9.1.4 注册定时器 1,用于触发当数据收集完成时,排序输出
            //TODO 9.1.5 注册定时器 2 用于触发状态清除
            //TODO 9.1.5 清空状态
            //TODO 9.1.4.1 取出状态中的数据 并将Lists.newArrayList(iterator) 转换成  ArrayList<IdCount>
            //TODO 9.1.4.2 排序
            //TODO 9.1.4.3 输出
        }
    }
}
