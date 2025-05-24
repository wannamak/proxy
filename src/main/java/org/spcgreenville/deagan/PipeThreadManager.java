package org.spcgreenville.deagan;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PipeThreadManager {
  private final Logger logger = Logger.getLogger(PipeThreadManager.class.getCanonicalName());

  private final List<PipeThread> pipeThreads = Collections.synchronizedList(new ArrayList<>());
  private final WriterThread writerThread;
  private int threadIndex = 0;

  public PipeThreadManager() {
    writerThread = new WriterThread(pipeThreads);
    writerThread.start();
  }

  public void setStop() {
    writerThread.setStop();
    try {
      writerThread.join();
      logger.info("Joined writer thread");
    } catch (InterruptedException ie) {
      logger.log(Level.WARNING, "Interrupted while awaiting writer thread join", ie);
    }
    List<PipeThread> pipeThreadsLocal;
    synchronized (pipeThreads) {
      pipeThreadsLocal = new ArrayList<>(pipeThreads);
    }
    for (PipeThread pipeThread : pipeThreadsLocal) {
      pipeThread.setStop();
      try {
        pipeThread.join();
      } catch (InterruptedException ie) {
        logger.log(Level.WARNING, "Interrupted while awaiting pipe thread join", ie);
      }
    }
  }

  public void createPipeThread(Socket socket) {
    PipeThread pipeThread = new PipeThread(socket, this, writerThread);
    synchronized (pipeThreads) {
      if (!pipeThread.init()) {
        logger.info("Unable to initialize pipe thread");
        return;
      }
      pipeThreads.add(pipeThread);
    }
    pipeThread.start();
    logger.info("Started pipe thread " + pipeThread.getName());
  }

  public void removePipeThread(PipeThread pipeThread) {
    synchronized (pipeThreads) {
      logger.info("Removing pipe thread " + pipeThread.getName());
      if (!pipeThreads.remove(pipeThread)) {
        throw new RuntimeException("Removing a thread that isn't there");
      }
    }
  }

  public int getThreadIndex() {
    try {
      return threadIndex;
    } finally {
      threadIndex++;
    }
  }
}
