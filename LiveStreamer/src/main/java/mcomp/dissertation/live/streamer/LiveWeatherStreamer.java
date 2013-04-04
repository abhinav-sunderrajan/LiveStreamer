package mcomp.dissertation.live.streamer;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.LiveWeatherBean;

public class LiveWeatherStreamer extends AbstractLiveStreamer<LiveWeatherBean> {

   private DateFormat df;

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
         final String serverIP, int serverPort) {
      super(streamRate, monitor, executor, folderLocation, dateString,
            serverIP, serverPort);
      this.df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss aaa");

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

}
