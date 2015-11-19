package org.indexing;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

class News {

	private String title;
	private String headline;
	private String text;
	private int id;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getHeadline() {
		return headline;
	}

	public void setHeadline(String headline) {
		this.headline = headline;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

}

public class XmlParser {
	public static String parse(BytesWritable value, Text key)throws IOException {	
		String tempitem = "";
		try {
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(
					new ByteArrayInputStream(value.getBytes()));

			doc.getDocumentElement().normalize();

			NodeList nList = doc.getElementsByTagName("newsitem");
			
			for (int temp = 0; temp < nList.getLength(); temp++) {
				Node nNode = nList.item(temp);

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {
					Element eElement = (Element) nNode;
					News tempNews = new News();
					tempNews.setTitle(eElement.getElementsByTagName("title").item(0).getTextContent().trim().replaceAll("\"", ""));
					tempNews.setHeadline(eElement.getElementsByTagName("headline").item(0).getTextContent().trim().replaceAll("\"", ""));
					tempNews.setText(eElement.getElementsByTagName("text").item(0).getTextContent().trim().replaceAll("\"", ""));
					tempNews.setId(Integer.parseInt(eElement.getAttribute("itemid")));
					String st1 = "itemid_" + String.valueOf(tempNews.getId());
					st1 = st1 + " " + tempNews.getTitle().toLowerCase();
					st1 = st1 + " " + tempNews.getText().toLowerCase();
					tempitem = st1;
					
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tempitem;
	}
}
