package mcomp.dissertation.live.streamer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;

import mcomp.dissertation.live.serverconnect.NettyServerConnect;

import org.apache.log4j.Logger;

/**
 * This abstract class is responsible for streaming live data from a CSV file.
 */
public abstract class AbstractLiveStreamer<E> {

   private File file;
   protected int streamRate;
   private ScheduledExecutorService executor;
   private String folderLocation;
   private String dateString;
   private ConcurrentLinkedQueue<E> buffer;
   private String serverIP;
   private int serverPort;
   protected BufferedReader br;
   private static final Logger LOGGER = Logger
         .getLogger(AbstractLiveStreamer.class);

   /**
    * 
    * @param streamRate
    * @param monitor
    * @param executor
    * @param folderLocation
    * @param dateString
    * @param serverIP
    * @param serverPort
    */
   public AbstractLiveStreamer(final int streamRate, final Object monitor,
         final ScheduledExecutorService executor, final String folderLocation,
         final String dateString, final String serverIP, final int serverPort,
         final ConcurrentLinkedQueue<E> buffer) {

      try {
         this.streamRate = streamRate;
         this.dateString = dateString;
         this.serverIP = serverIP;
         this.folderLocation = folderLocation;
         this.file = readFileData();
         this.executor = executor;
         this.serverPort = serverPort;
         this.buffer = buffer;
         this.br = new BufferedReader(new FileReader(file));
         Runtime.getRuntime().addShutdownHook(new ShutDownHook());
         createServerSettings();

      } catch (IOException e) {
         LOGGER.error("Error parsing the file", e);

      } catch (InterruptedException e) {
         LOGGER.error("Unable to connect to server..", e);

      } catch (Exception e) {
         LOGGER.error("Error finding the required file to stream off..", e);
      }
   }

   private void createServerSettings() throws InterruptedException {

      NettyServerConnect<E> send = new NettyServerConnect<E>(serverIP, buffer,
            executor, streamRate);
      send.connectToNettyServer(serverPort);

   }

   /**
    * 
    * @return
    * @throws Exception
    */
   protected File readFileData() throws Exception {
      File dir = new File(folderLocation);
      LOGGER.info("Reading live data from " + dir.getAbsolutePath());
      File[] files = dir.listFiles(new FileFilter() {

         public boolean accept(final File pathname) {
            String fileName = pathname.getName();

            if (fileName.startsWith(".") || (!fileName.endsWith(".csv"))) {
               return false;
            } else {

               @SuppressWarnings("deprecation")
               Date dataDate = new Date(dateString);

               Date fileDate = null;
               fileDate = getDateFromFileName(fileName.substring(8, 18));

               if (dataDate.equals(fileDate)) {
                  return true;
               } else {
                  return false;
               }

            }
         }

      });
      if (files != null) {
         return files[0];
      } else {
         throw new Exception(
               "Unable to initialize file data - check directory path.");
      }

   }

   @SuppressWarnings("deprecation")
   private Date getDateFromFileName(final String dateString) {
      String date = dateString.substring(0, 4) + "/"
            + dateString.substring(5, 7) + "/" + dateString.substring(8, 10);
      return new Date(date);

   }

   /**
    * 
    * @param line
    * @return the parsed record from the file as a Java bean object
    */
   protected abstract E parseLine(final String line);

   protected abstract void startBufferThread();

   /**
    * 
    * Call on JVM exit to close the buffered reader connections.
    * 
    */
   private class ShutDownHook extends Thread {

      public void run() {
         LOGGER.info("Closing the buffered reader before exit..");
         try {
            br.close();
         } catch (IOException e) {
            LOGGER.error("Error closing the buffered reader", e);
         }
      }

   }

}
