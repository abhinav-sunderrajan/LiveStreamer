package mcomp.dissertation.live.streamer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import mcomp.dissertation.beans.LiveTrafficBean;
import mcomp.dissertation.beans.LiveWeatherBean;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ClassResolvers;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

public class LocalNettyServer {

   public static void main(String[] args) throws Exception {
      ChannelFactory factory = new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

      ServerBootstrap bootstrap = new ServerBootstrap(factory);

      bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
         public ChannelPipeline getPipeline() {
            return Channels.pipeline(
                  new ObjectDecoder(ClassResolvers.cacheDisabled(getClass()
                        .getClassLoader())), new ObjectEncoder(),
                  new FirstHandshake());
         }
      });

      bootstrap.setOption("child.tcpNoDelay", true);
      bootstrap.setOption("child.keepAlive", true);

      bootstrap.bind(new InetSocketAddress(8080));
      bootstrap.bind(new InetSocketAddress(9090));
      System.out.println("Started the server");
   }

   private static class FirstHandshake extends SimpleChannelHandler {
      private int count = 0;

      @Override
      public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception {
         Channel channel = e.getChannel();
         if (e.getMessage() instanceof String) {
            String msg = (String) e.getMessage();
            if (msg.equalsIgnoreCase("A0092715")) {
               ChannelFuture channelFuture = Channels.future(e.getChannel());
               ChannelEvent responseEvent = new DownstreamMessageEvent(channel,
                     channelFuture, "gandu", channel.getRemoteAddress());
               ctx.sendDownstream(responseEvent);
               super.messageReceived(ctx, e);

            }
         } else {
            Object obj = e.getMessage();
            count++;
            LiveTrafficBean traffic;
            LiveWeatherBean weather;
            if (obj instanceof LiveTrafficBean) {
               traffic = (LiveTrafficBean) obj;
               if (count % 1000 == 0) {
                  System.out.println("Speed on " + traffic.getLinkId() + " at "
                        + traffic.getTimeStamp() + " is " + traffic.getSpeed());

               }

            } else {
               weather = (LiveWeatherBean) obj;
               if (count % 1000 == 0) {
                  System.out.println("Rain at " + weather.getLinkId() + " at "
                        + weather.getTimeStamp() + " is " + weather.getRain());

               }

            }
         }

      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
         e.getCause().printStackTrace();
         e.getChannel().close();
      }
   }

}
