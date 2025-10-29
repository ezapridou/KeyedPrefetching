package net.michaelkoepf.spegauge.driver.webapi;


import lombok.Data;

import java.util.UUID;

@Data
public class JobCopyRequest {
  private UUID jobId;
  private int numCopies;


  public JobCopyRequest(String toBeCopied, int numCopies) {
    this.jobId = UUID.fromString(toBeCopied);
    this.numCopies = numCopies;
  }
}
