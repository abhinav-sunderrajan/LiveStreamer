package mcomp.dissertation.live;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.beans.LiveWeatherBean;
import mcomp.dissertation.live.streamer.AbstractLiveStreamer;
import mcomp.dissertation.live.streamer.LiveTrafficStreamer;
import mcomp.dissertation.live.streamer.LiveWeatherStreamer;
import net.sourceforge.sizeof.SizeOf;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public final class StreamerCore {

   private ScheduledExecutorService executor;
   private Object monitor;
   private int numberOfStreams;
   private AbstractLiveStreamer<?>[] streamers;
   private SAXReader reader;
   private static AtomicInteger streamRate;
   private static Properties configProperties;
   private static StreamerCore core;
   private static String xmlFilePath;
   private static final String XML_FILE_PATH = "src/main/resources/streams.xml";
   private static final String CONFIG_FILE_PATH = "src/main/resources/config.properties";
   private static final Logger LOGGER = Logger.getLogger(StreamerCore.class);

   /**
    * 
    * @param configFilePath
    * @throws IOException
    * @throws FileNotFoundException
    */
   private StreamerCore(final String configFilePath)
         throws FileNotFoundException, IOException {
      configProperties = new Properties();
      configProperties.load(new FileInputStream(configFilePath));
      streamRate = new AtomicInteger(Integer.parseInt(configProperties
            .getProperty("live.stream.rate.in.microsecs")));
      numberOfStreams = Integer.parseInt(configProperties
            .getProperty("number.of.streams"));
      executor = Executors.newScheduledThreadPool(numberOfStreams);
      streamers = new AbstractLiveStreamer<?>[numberOfStreams];
      monitor = new Object();

   }

   /**
    * Call to return instance of Streamer
    * @param configFilePath
    * @return
    */
   public static StreamerCore getStreamerIntsance(final String configFilePath) {
      if (core == null) {
         try {
            core = new StreamerCore(configFilePath);
         } catch (FileNotFoundException e) {
            LOGGER.error("Unable to find the config file", e);
         } catch (IOException e) {
            LOGGER.error("Properties file contains non unicode values ", e);
         }
         return core;
      } else {
         return core;
      }

   }

   public static void main(String[] args) {

      String configFilePath;
      if (args.length < 2) {
         configFilePath = CONFIG_FILE_PATH;
         xmlFilePath = XML_FILE_PATH;

      } else {
         configFilePath = args[0];
         xmlFilePath = args[1];

      }
      try {
         core = StreamerCore.getStreamerIntsance(configFilePath);
         core.startLiveStreams();
      } catch (FileNotFoundException e) {
         LOGGER.error("Unable to find xml file containing stream info", e);
      } catch (DocumentException e) {
         LOGGER.error("Erroneous stream info xml file. Please check", e);
      } catch (InterruptedException e) {
         LOGGER.error(
               "Error in delaying the traffic data stream wrt to weather data stream",
               e);
      }
   }

   @SuppressWarnings("unchecked")
   private void startLiveStreams() throws FileNotFoundException,
         DocumentException, InterruptedException {
      reader = new SAXReader();
      InputStream configxml = new FileInputStream(xmlFilePath);
      reader = new SAXReader();
      Document doc = reader.read(configxml);
      Element docRoot = doc.getRootElement();
      List<Element> streams = docRoot.elements();
      int count = 0;
      for (Element stream : streams) {
         String serverIP = stream.attribute(0).getText();
         int serverPort = Integer.parseInt(stream.attribute(1).getText());
         if (stream.elementText("streamname").equalsIgnoreCase("weather")) {
            ConcurrentLinkedQueue<LiveWeatherBean> weatherBuffer = new ConcurrentLinkedQueue<LiveWeatherBean>();
            LOGGER.info("Size of one weather payload in bytes is "
                  + SizeOf.deepSizeOf(new LiveWeatherBean()));
            streamers[count] = new LiveWeatherStreamer(
                  (int) (streamRate.get() * Float.parseFloat(configProperties
                        .getProperty("weather.speed.up"))), monitor, executor,
                  configProperties.getProperty("weather.live.data.folder"),
                  configProperties.getProperty("live.data.date"), serverIP,
                  serverPort, weatherBuffer);
            // Giving a small head start for the weather stream since traffic
            // stream is unidirectional join.
            Thread.sleep(2000);

         } else {
            ConcurrentLinkedQueue<LiveTrafficBean> trafficBuffer = new ConcurrentLinkedQueue<LiveTrafficBean>();
            LOGGER.info("Size of one traffic payload in bytes is "
                  + SizeOf.deepSizeOf(new LiveTrafficBean()));
            streamers[count] = new LiveTrafficStreamer(streamRate.get(),
                  monitor, executor,
                  configProperties.getProperty("traffic.live.data.folder"),
                  configProperties.getProperty("live.data.date"), serverIP,
                  serverPort, trafficBuffer);

         }

      }
   }
}
