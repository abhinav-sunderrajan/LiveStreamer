package mcomp.dissertation.live.streamer;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.LiveTrafficBean;

import org.apache.log4j.Logger;

/**
 * 
 * The subclass for streaming live traffic data over the network.
 * 
 */
public class LiveTrafficStreamer extends AbstractLiveStreamer<LiveTrafficBean> {

   private DateFormat df;
   private DateFormat dfLocal;
   private ConcurrentLinkedQueue<LiveTrafficBean> buffer;
   private static final Logger LOGGER = Logger
         .getLogger(LiveTrafficStreamer.class);

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
   public LiveTrafficStreamer(final AtomicInteger streamRate,
         final Object monitor, final ScheduledExecutorService executor,
         final String folderLocation, final String dateString,
         final String serverIP, final int serverPort,
         final ConcurrentLinkedQueue<LiveTrafficBean> trafficBuffer) {
      super(streamRate, monitor, executor, folderLocation, dateString,
            serverIP, serverPort, trafficBuffer);
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
      this.dfLocal = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
      this.buffer = trafficBuffer;
      // Change added since the streaming strategy varies for traffic and
      // weather streams.
      startBufferThread();

   }

   @Override
   protected LiveTrafficBean parseLine(String line) {
      LiveTrafficBean bean = new LiveTrafficBean();
      try {
         bean.setEventTime(dfLocal.format(Calendar.getInstance().getTime()));
         String[] items = line.split("\\|");
         bean.setLinkId(Integer.parseInt(items[0].trim()));

         Date time;
         time = df.parse(items[1].trim());
         bean.setTimeStamp(new Timestamp(time.getTime()));
         // Check if speed is null
         if (items[2].trim().equals("") || items[2].trim() == null) {
            bean.setAvgSpeed(0);
         } else {
            bean.setAvgSpeed(Float.parseFloat(items[2].trim()));

         }
         // Check if volume is null
         if (items[3].trim().equals("") || items[3].trim() == null) {
            bean.setAvgVolume(0);
         } else {
            bean.setAvgVolume(Integer.parseInt(items[3].trim()));
         }

      } catch (Exception e) {
         e.printStackTrace();
      }

      return bean;

   }

   @Override
   protected void startBufferThread() {
      Thread bufferThread = new Thread(new AddToBuffer());
      bufferThread.setDaemon(true);
      bufferThread.start();

   }

   /**
    * 
    * This thread reads data of the parsed file and adds it to the buffer.
    * 
    */
   private class AddToBuffer implements Runnable {

      public void run() {
         try {
            while (br.ready()) {
               LiveTrafficBean bean = parseLine(br.readLine());
               buffer.add(bean);
            }
         } catch (IOException e) {
            LOGGER.error("Error reading a record from live traffic CSV file", e);
         }

      }

   }

}
