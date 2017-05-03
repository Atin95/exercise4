package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender 
{
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;
	
	private ActiveMQConnectionFactory connectionFactory;
	private Connection c;

	public JmsSender(final String queueName, final String topicName) 
	{
		this.queueName = queueName;
		this.topicName = topicName;
		
		this.connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:62616");
		
		try 
		{
			c = connectionFactory.createConnection();
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
	}

	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) 
	{
		try 
		{
			c.start();
			
			Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			ObjectMessage om = s.createObjectMessage(new Order(orderId, product, price));
			om.setJMSType("Order");
			om.setStringProperty("WDSR-System", "OrderProcessor");
			
			Destination d = s.createQueue(queueName);
			MessageProducer mp = s.createProducer(d);
			
			mp.send(om);
			s.close();
			c.close();
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) 
	{
		try
		{
			c.start();
			
			Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			TextMessage tm = s.createTextMessage(text);
			tm.setJMSType("Order");
			tm.setStringProperty("WDSR-System", "OrderProcessor");
			
			Destination d = s.createQueue(queueName);
			MessageProducer mp = s.createProducer(d);
			
			mp.send(tm);
			s.close();
			c.close();
		}
		catch (JMSException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) 
	{
		try 
		{
			c.start();
			
			Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			MapMessage mm = s.createMapMessage();
			for (Map.Entry<String, String> e : map.entrySet())
				mm.setString(e.getKey(), e.getValue());
			mm.setJMSType("Order");
			mm.setStringProperty("WDSR-System", "OrderProcessor");
			
			Destination d = s.createTopic(topicName);
			MessageProducer mp = s.createProducer(d);
			
			mp.send(mm);
			s.close();
			c.close();
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
	}
}
