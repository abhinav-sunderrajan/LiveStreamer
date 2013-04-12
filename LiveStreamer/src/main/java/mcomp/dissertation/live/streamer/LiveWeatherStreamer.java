package mcomp.dissertation.live.streamer;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.LiveWeatherBean;

import org.apache.log4j.Logger;

public class LiveWeatherStreamer extends AbstractLiveStreamer<LiveWeatherBean> {

   private DateFormat df;
   private ConcurrentLinkedQueue<LiveWeatherBean> buffer;
   private static final Logger LOGGER = Logger
         .getLogger(LiveWeatherStreamer.class);
   private Queue<LiveWeatherBean> replayQueue;

   /**
    * 
    * @param streamRate
    * @param df
    * @param monitor
    * @param executor
    * @param folderLocation
    * @param dateString
    * @param serverIP
    * @param serverPort
    */
   public LiveWeatherStreamer(final AtomicInteger streamRate,
         final Object monitor, final ScheduledExecutorService executor,
         final String folderLocation, final String dateString,
         final String serverIP, int serverPort,
         ConcurrentLinkedQueue<LiveWeatherBean> weatherBuffer) {
      super(streamRate, monitor, executor, folderLocation, dateString,
            serverIP, serverPort, weatherBuffer);
      this.df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      this.replayQueue = new LinkedBlockingQueue<LiveWeatherBean>();
      this.buffer = weatherBuffer;
      startBufferThread();

   }

   @Override
   protected LiveWeatherBean parseLine(String line) {
      LiveWeatherBean bean = new LiveWeatherBean();
      try {

         String[] items = line.split(",");
         bean.setLinkId(Integer.parseInt(items[0].trim()));

         Date time;
         time = df.parse(items[3].trim());
         bean.setTimeStamp(new Timestamp(time.getTime()));
         // Check if rainfall is null
         if (items[2].trim().equals("") || items[2].trim() == null) {
            bean.setRain(0);
         } else {
            bean.setRain(Double.parseDouble(items[2].trim()));

         }
         // Check if temperature is null
         if (items[1].trim().equals("") || items[1].trim() == null) {
            bean.setTemperature(0);
         } else {
            bean.setTemperature(Double.parseDouble(items[1].trim()));
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
      private int count;
      private Timestamp track;
      private boolean flag;

      AddToBuffer() {
         count = 0;
         flag = false;
      }

      public void run() {
         try {
            while (br.ready()) {
               LiveWeatherBean bean = parseLine(br.readLine());
               if (!flag) {
                  track = bean.getTimeStamp();
               }
               flag = true;
               if (bean.getTimeStamp().getTime() == track.getTime()) {
                  buffer.add(bean);
                  replayQueue.add(bean);
               } else {
                  while (count < 5) {
                     Iterator<LiveWeatherBean> it = replayQueue.iterator();
                     while (it.hasNext()) {
                        buffer.add(it.next());
                     }
                     System.out.println(count);
                     count++;
                  }
                  LOGGER.info("Done with replay starting to stream records from "
                        + bean.getTimeStamp());
                  replayQueue.clear();
                  buffer.add(bean);
                  replayQueue.add(bean);
                  track = bean.getTimeStamp();
                  flag = false;
                  count = 0;

               }

            }
         } catch (IOException e) {
            LOGGER.error("Error reading a record from live traffic CSV file", e);
         }

      }
   }

}
