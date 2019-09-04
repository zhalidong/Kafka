package com.atguigu.kafka.producer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestFuture {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //线程池
        ExecutorService executor = Executors.newCachedThreadPool();
        //提交一个线程
        Future<?> future = executor.submit(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    System.out.println("i = " + i);
                }
            }
        });
        future.get();//线程会阻塞 等提交的线程跑完之后才会继续执行下面的代码

        System.out.println("==================================");
        //关闭线程池
        executor.shutdown();

    }

}
