package net.michaelkoepf.spegauge.flink.queries.sqbench.core.function;

import net.michaelkoepf.spegauge.api.common.model.sqbench.EntityRecordFull;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class TestAsyncFunction extends RichAsyncFunction<EntityRecordFull, EntityRecordFull> {

  public TestAsyncFunction() {
  }

  @Override
  public void asyncInvoke(EntityRecordFull input, ResultFuture<EntityRecordFull> resultFuture) throws Exception {
      final CompletableFuture<EntityRecordFull> future = new CompletableFuture<>() {
        final long start = System.nanoTime();

        @Override
        public EntityRecordFull get() throws InterruptedException, ExecutionException {
          while (System.nanoTime() - start < 2e8) {
            // busy wait
          }
          System.out.println("Async function completed");
          return input;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return false;
        }
      };

      CompletableFuture.supplyAsync(new Supplier<EntityRecordFull>() {
        @Override
        public EntityRecordFull get() {
          try {
            return future.get();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }).thenAccept( (EntityRecordFull entityRecord) -> {
        resultFuture.complete(Collections.singleton(entityRecord));
      });
  }
}
