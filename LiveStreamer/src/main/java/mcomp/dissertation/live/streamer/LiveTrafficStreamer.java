package mcomp.dissertation.live.streamer;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;

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
   private int streamRate;
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
   public LiveTrafficStreamer(final int streamRate, final Object monitor,
         final ScheduledExecutorService executor, final String folderLocation,
         final String dateString, final String serverIP, final int serverPort,
         final ConcurrentLinkedQueue<LiveTrafficBean> trafficBuffer) {
      super(streamRate, monitor, executor, folderLocation, dateString,
            serverIP, serverPort, trafficBuffer);
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
      this.dfLocal = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.SSS");
      this.buffer = trafficBuffer;
      this.streamRate = streamRate;
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
            bean.setSpeed(0);
         } else {
            bean.setSpeed(Float.parseFloat(items[2].trim()));

         }
         // Check if volume is null
         if (items[3].trim().equals("") || items[3].trim() == null) {
            bean.setVolume(0);
         } else {
            bean.setVolume(Integer.parseInt(items[3].trim()));
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
      private int bufferCount;
      private Timestamp track;

      public AddToBuffer() {
         try {
            bufferCount = 0;
            DateFormat df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
            track = new Timestamp(df.parse("17-Apr-2011 00:00:00").getTime());
         } catch (ParseException e) {
            LOGGER.error("Error intializing start date for traffic stream");
         }
      }

      public void run() {
         try {
            while (br.ready()) {
               LiveTrafficBean bean = parseLine(br.readLine());
               if (bean.getTimeStamp().getTime() - track.getTime() == (30 * 60 * 1000)) {
                  LOGGER.info("Finished streaming records at " + track
                        + " sending cti");
                  LiveTrafficBean cti = new LiveTrafficBean();
                  cti.setLinkId(999999999);
                  cti.setTimeStamp(track);
                  buffer.add(cti);
                  track = bean.getTimeStamp();
               }
               buffer.add(bean);
               bufferCount++;
               if (bufferCount % 20000 == 0) {
                  Thread.sleep(streamRate);
               }
            }
         } catch (IOException e) {
            LOGGER.error("Error reading a record from live traffic CSV file", e);
         } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting", e);
         }

      }

   }

}
