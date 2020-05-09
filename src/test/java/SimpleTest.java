import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Iterator;

/**
 * @author You
 * @Description
 * @create 2020-05-08 09:42
 **/
public class SimpleTest {
    @Test
    public void test1() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Integer> input = env.fromElements(1, 2, 3);
        input.map(i -> i * i).print();

        input.flatMap((Integer number, Collector<String> out) -> {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < number; i++) {
                builder.append("a");
                // prints "a", "a", "aa", "a", "aa", "aaa"
//                out.collect(builder.toString());
            }
            out.collect(builder.toString());//prints a,aa,aaa
        }).returns(Types.STRING)// provide type information explicitly
                .print();
    }

    @Test
    //关于泛型类型擦除的问题
    public void test2() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> input = env.fromElements(1, 2, 3);

        //1.
        input.map(i -> Tuple2.of(i, i)) // no information about fields of Tuple2
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();

        //2.
        input.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
                return Tuple2.of(integer, integer);
            }
        }).print();

        //3.
        input.map(i -> new MyTuple2Mapper()).print();

        //4.
        input.map(i -> new DoubleTuple(i, i)).print();

    }

    public static class MyTuple2Mapper implements MapFunction<Integer, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer, Integer> map(Integer integer) {
            return Tuple2.of(integer, integer);
        }
    }

    public static class DoubleTuple extends Tuple2<Integer, Integer> {
        public DoubleTuple(){}
        public DoubleTuple(int f0, int f1) {
            this.f0 = f0;
            this.f1 = f1;
        }
    }

    @Test
    public void test3() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> someIntegers = env.generateSequence(0, 1);
        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero).print().setParallelism(1);

        env.execute();
    }
}

