package org.spcgreenville.deagan;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WriterThread extends Thread {
  private final Logger logger = Logger.getLogger(WriterThread.class.getCanonicalName());
  private final Deque<QueueEntry> outboundQueue = new ArrayDeque<>();
  private final List<PipeThread> pipeThreads;
  private final AtomicBoolean shouldStop = new AtomicBoolean(false);

  // We do this to prevent echo.
  public static class QueueEntry {
    public int i;
    public PipeThread exceptThisThread;
    public QueueEntry(int i, PipeThread exceptThisThread) {
      this.i = i;
      this.exceptThisThread = exceptThisThread;
    }
  }

  public WriterThread(List<PipeThread> pipeThreads) {
    this.pipeThreads = pipeThreads;
    setDaemon(true);
    setName("WriterThread");
    logger.info("Constructed writer thread");
  }

  public void write(int i, PipeThread exceptThisThread) {
    synchronized (outboundQueue) {
      outboundQueue.push(new QueueEntry(i, exceptThisThread));
      outboundQueue.notify();
    }
  }

  public void setStop() {
    shouldStop.set(true);
    synchronized (outboundQueue) {
      outboundQueue.notify();
    }
  }

  public void run() {
    while (!shouldStop.get()) {
      synchronized (outboundQueue) {
        if (outboundQueue.isEmpty()) {
          logger.fine("Awaiting queue notification");
          awaitQueueNotification();
        }
      }

      if (outboundQueue.isEmpty()) {
        return;  // thread is stopping
      }
      QueueEntry entry = outboundQueue.pop();
      logger.fine("Got entry " + entry.i + " from " + entry.exceptThisThread);
      synchronized (pipeThreads) {
        List<PipeThread> toRemove = new ArrayList<>();
        for (PipeThread pipeThread : pipeThreads) {
          if (pipeThread.equals(entry.exceptThisThread)) {
            continue;
          }
          if (!pipeThread.write(entry.i)) {
            toRemove.add(pipeThread);
          }
        }
        pipeThreads.removeAll(toRemove);
      }
    }
  }

  private void awaitQueueNotification() {
    while (!shouldStop.get()) {
      try {
        outboundQueue.wait();
        return;
      } catch (InterruptedException ie) {
        logger.log(Level.WARNING, "Interrupted while waiting for queue notification", ie);
      }
    }
  }
}
