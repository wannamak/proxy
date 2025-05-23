package org.spcgreenville.deagan;

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;

public class ProxyTest {
  private final SocketAddress proxyAddress;

  public ProxyTest() throws UnknownHostException {
    proxyAddress = new InetSocketAddress(InetAddress.getLocalHost(), Proxy.PORT);
  }

  @Test
  public void testMainThreadShutdown() throws Exception {
    runProxyThread(() -> null);
  }

  public void runProxyThread(Callable<Void> testCallable) throws Exception {
    Proxy proxy = new Proxy();
    Thread proxyThread = null;
    try {
      proxyThread = new Thread(proxy);
      proxyThread.setDaemon(false);
      proxyThread.start();

      Socket connectionTester;
      while (true) {
        try {
          connectionTester = new Socket();
          connectionTester.connect(proxyAddress);
          break;
        } catch (IOException ioe) {
          Thread.sleep(100);
        }
      }
      connectionTester.close();

      testCallable.call();
    } finally {
      proxy.setStop();
      proxyThread.join();
    }
  }

  @Test
  public void testForward() throws Exception {
    runProxyThread(() -> {
      try (Socket client1 = new Socket()) {
        client1.connect(proxyAddress);
        try (Socket client2 = new Socket()) {
          client2.connect(proxyAddress);
          client1.getOutputStream().write(1);
          assertEquals(1, client2.getInputStream().read());
          client2.getOutputStream().write(2);
          assertEquals(2, client1.getInputStream().read());
        }
      }
      return null;
    });
  }

  @Test
  public void testDisconnect() throws Exception {
    runProxyThread(() -> {
      try (Socket client1 = new Socket()) {
        client1.connect(proxyAddress);
        try (Socket client2 = new Socket()) {
          client2.connect(proxyAddress);

          client1.getOutputStream().write(1);
          assertEquals(1, client2.getInputStream().read());
          client2.getOutputStream().write(2);
          assertEquals(2, client1.getInputStream().read());

          client1.close();

          try (Socket client3 = new Socket()) {
            client3.connect(proxyAddress);

            client3.getOutputStream().write(1);
            assertEquals(1, client2.getInputStream().read());
            client2.getOutputStream().write(2);
            assertEquals(2, client3.getInputStream().read());
          }
        }
      }
      return null;
    });
  }
}