package org.spcgreenville.deagan;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a simple multicast proxy.
 * As clients connect to the server socket, any writes are sent to all other clients.
 */
public class Proxy implements Runnable {
  private final Logger logger = Logger.getLogger(Proxy.class.getCanonicalName());

  public final static int PORT = 8075;

  public static void main(String[] args)  {
    new Proxy().run();
  }

  private final AtomicBoolean shouldStop = new AtomicBoolean(false);
  private ServerSocket serverSocket;

  public void setStop() {
    shouldStop.set(true);
    synchronized (this) {
      if (serverSocket != null) {
        try {
          logger.info("Closing server socket");
          serverSocket.close();
        } catch (IOException ioe) {
          logger.log(Level.INFO, "Attempted to close server socket to unblock accept, but failed", ioe);
        }
      }
    }
  }

  public void run() {
    PipeThreadManager pipeManager = new PipeThreadManager();

    while (!shouldStop.get()) {
      try {
        synchronized (this) {
          serverSocket = new ServerSocket(PORT);
        }
        serverSocket.setSoTimeout(0);
        while (!shouldStop.get()) {
          pipeManager.createPipeThread(serverSocket.accept());
        }
      } catch (IOException ioe) {
        // Don't log expected shutdown message
        if (!shouldStop.get()) {
          logger.log(Level.WARNING, "Exception", ioe);
        }
      }
    }

    pipeManager.setStop();
  }
}