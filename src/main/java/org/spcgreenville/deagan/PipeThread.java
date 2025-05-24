package org.spcgreenville.deagan;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PipeThread extends Thread {
  private final Logger logger = Logger.getLogger(PipeThread.class.getCanonicalName());
  private final Socket socket;
  private final PipeThreadManager pipeThreadManager;
  private final WriterThread writerThread;
  private final AtomicBoolean shouldStop = new AtomicBoolean(false);

  private InputStream inputStream;
  private OutputStream outputStream;


  public PipeThread(Socket socket, PipeThreadManager pipeThreadManager, WriterThread writerThread) {
    setDaemon(true);
    setName("PipeThread-" + pipeThreadManager.getThreadIndex());
    this.socket = socket;
    this.pipeThreadManager = pipeThreadManager;
    this.writerThread = writerThread;
  }

  public boolean init() {
    try {
      this.inputStream = socket.getInputStream();
    } catch (IOException ioe) {
      logger.log(Level.WARNING, "Error getting input stream", ioe);
      return false;
    }
    try {
      this.outputStream = socket.getOutputStream();
    } catch (IOException ioe) {
      logger.log(Level.WARNING, "Error getting output stream", ioe);
      return false;
    }
    return true;
  }

  public void setStop() {
    shouldStop.set(true);
    try {
      inputStream.close();
    } catch (IOException ioe) {
      logger.log(Level.WARNING, "Error closing input stream", ioe);
    }
    try {
      outputStream.close();
    } catch (IOException ioe) {
      logger.log(Level.WARNING, "Error closing output stream", ioe);
    }
  }

  public boolean write(int i) {
    try {
      logger.fine("Writing " + i + " to " + getName());
      outputStream.write(i);
      return true;
    } catch (IOException ioe) {
      logger.log(Level.WARNING, "Error writing output stream", ioe);
      return false;
    }
  }

  public void run() {
    while (!shouldStop.get()) {
      try {
        int i = inputStream.read();
        logger.fine("Read " + i + " from " + getName());
        if (i == -1) {
          logger.warning("Input stream closed for pipe " + getName());
          pipeThreadManager.removePipeThread(this);
          return;
        }
        writerThread.write(i, this);
      } catch (IOException ioe) {
        logger.log(Level.WARNING, "Error reading input stream", ioe);
        pipeThreadManager.removePipeThread(this);
        return;
      }
    }
  }
}
