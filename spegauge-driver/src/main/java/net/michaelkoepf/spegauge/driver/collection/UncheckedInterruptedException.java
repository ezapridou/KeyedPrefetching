package net.michaelkoepf.spegauge.driver.collection;

public class UncheckedInterruptedException extends RuntimeException {

      public UncheckedInterruptedException(InterruptedException e) {
          super(e);
      }
}
