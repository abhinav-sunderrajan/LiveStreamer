package mcomp.dissertation.live.streamer;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;

import mcomp.dissertation.beans.LiveWeatherBean;

import org.apache.log4j.Logger;

public class LiveWeatherStreamer extends AbstractLiveStreamer<LiveWeatherBean> {

   private DateFormat df;
   private ConcurrentLinkedQueue<LiveWeatherBean> buffer;
   private int streamRate;
   private static final Logger LOGGER = Logger
         .getLogger(LiveWeatherStreamer.class);

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
   public LiveWeatherStreamer(final int streamRate, final Object monitor,
         final ScheduledExecutorService executor, final String folderLocation,
         final String dateString, final String serverIP, int serverPort,
         ConcurrentLinkedQueue<LiveWeatherBean> weatherBuffer) {
      super(streamRate, monitor, executor, folderLocation, dateString,
            serverIP, serverPort, weatherBuffer);
      this.df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      this.buffer = weatherBuffer;
      this.streamRate = streamRate;
      startBufferThread();

   }

   @Override
   protected LiveWeatherBean parseLine(String line) {
      LiveWeatherBean bean = new LiveWeatherBean();
      try {

         String[] items = line.split(",");
         bean.setLinkId(Integer.parseInt(items[0].trim()));

         Date time;
         if (items[3].trim().equals("") || items[3].trim() == null) {
            time = null;
         } else {
            time = df.parse(items[3].trim());
            bean.setTimeStamp(new Timestamp(time.getTime()));
         }
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
      private int bufferCount;
      private Timestamp track;

      public AddToBuffer() {
         try {
            bufferCount = 0;
            DateFormat df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");
            track = new Timestamp(df.parse("17-Apr-2011 00:00:00").getTime());
         } catch (ParseException e) {
            LOGGER.error("Error intializing start date for weather stream");
         }

      }

      public void run() {
         try {
            while (br.ready()) {
               LiveWeatherBean bean = parseLine(br.readLine());
               if (bean.getTimeStamp().compareTo(track) > 0) {
                  LOGGER.info("Finished streaming records at time " + track
                        + "wait for next update after 30 minutes");
                  track = bean.getTimeStamp();

                  // The logic behind this wait is based on the number of
                  // milli-seconds it takes to stream one burst and wait for 5
                  // times that time so as to make for the traffic stream.
                  Thread.sleep(5 * streamRate * bufferCount / 1000);
                  bufferCount = 0;
               }
               buffer.add(bean);
               bufferCount++;
            }
         } catch (IOException e) {
            LOGGER.error("Error reading a record from live traffic CSV file", e);
         } catch (InterruptedException e) {
            LOGGER.error("interrupted while waiting");
         }

      }
   }

}
