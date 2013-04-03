package mcomp.dissertation.live.streamer;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.live.beans.LiveTrafficBean;

/**
 * 
 * The subclass for streaming live traffic data over the network.
 * 
 */
public class LiveTrafficStreamer extends AbstractLiveStreamer<LiveTrafficBean> {

   private DateFormat df;

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
         final String serverIP, final int serverPort) {
      super(streamRate, monitor, executor, folderLocation, dateString,
            serverIP, serverPort);
      this.df = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

   }

   @Override
   protected LiveTrafficBean parseLine(String line) {
      LiveTrafficBean bean = new LiveTrafficBean();
      try {

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

}
