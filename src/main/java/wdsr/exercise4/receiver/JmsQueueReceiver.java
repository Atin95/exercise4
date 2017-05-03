package wdsr.exercise4.receiver;

import java.math.BigDecimal;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver 
{
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	
	private ActiveMQConnectionFactory connectionFactory;
	private Connection c;
	private Session s;
	private Destination d;
	private MessageConsumer mc;
	
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName) 
	{
		connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");
		connectionFactory.setTrustAllPackages(true);
		try 
		{
			c = connectionFactory.createConnection();
			c.start();
			s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
			d = s.createQueue(queueName);
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
	}

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) 
	{
		try 
		{
			mc = s.createConsumer(d);
			mc.setMessageListener(new JmsQueueReceiverListener(alertService));
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown()
	{
		try 
		{
			s.close();
			c.close();
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
	}

	// TODO
	// This object should start consuming messages when registerCallback method is invoked.
	
	// This object should consume two types of messages:
	// 1. Price alert - identified by header JMSType=PriceAlert - should invoke AlertService::processPriceAlert
	// 2. Volume alert - identified by header JMSType=VolumeAlert - should invoke AlertService::processVolumeAlert
	// Use different message listeners for and a JMS selector 
	
	// Each alert can come as either an ObjectMessage (with payload being an instance of PriceAlert or VolumeAlert class)
	// or as a TextMessage.
	// Text for PriceAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Price=<long value>
	// Text for VolumeAlert looks as follows:
	//		Timestamp=<long value>
	//		Stock=<String value>
	//		Volume=<long value>
	
	// When shutdown() method is invoked on this object it should remove the listeners and close open connection to the broker.   
}

class JmsQueueReceiverListener implements MessageListener
{
	private AlertService as;
	
	public JmsQueueReceiverListener(AlertService as)
	{
		this.as = as;
	}

	@Override
	public void onMessage(Message message) 
	{
		if (message.getClass() == ObjectMessage.class)
		{
			ObjectMessage om = (ObjectMessage)message;
			try 
			{
				if (om.getJMSType() == "PriceAlert")
					as.processPriceAlert((PriceAlert)om.getObject());
				else if (om.getJMSType() == "VolumeAlert")
					as.processVolumeAlert((VolumeAlert)om.getObject());
			} 
			catch (JMSException e)
			{
				e.printStackTrace();
			}
		}
		else if (message.getClass() == TextMessage.class)
		{
			TextMessage tm = (TextMessage)message;
			try 
			{
				String lines[] = tm.getText().split("\\r?\\n");
				
				Long timestamp = Long.parseLong(lines[0].split("=")[1]);
				String stock = lines[1].split("=")[1];
				Long priceOrVolume = Long.parseLong(lines[2].split("=")[1]);
				
				if (tm.getJMSType() == "PriceAlert")
					as.processPriceAlert(new PriceAlert(timestamp, stock, BigDecimal.valueOf(priceOrVolume)));
				else if (tm.getJMSType() == "VolumeAlert")
					as.processVolumeAlert(new VolumeAlert(timestamp, stock, priceOrVolume));
			} 
			catch (JMSException e) 
			{
				e.printStackTrace();
			}
		}
	}
}
