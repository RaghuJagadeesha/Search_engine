package xmltext;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class News{

	private String title;
	private String headline;
	private String byline;
	private String Dateline;
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
	public String getByline() {
		return byline;
	}
	public void setByline(String byline) {
		this.byline = byline;
	}
	public String getDateline() {
		return Dateline;
	}
	public void setDateline(String dateline) {
		Dateline = dateline;
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

public class xmltotext {
	public static void main(String[] args) {

		try {
			File file = new File("okapi_precision/part-r-00000");
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String outtext = null;
			List<String> list = new ArrayList<String>();
			while((outtext = reader.readLine()) != null){
				String[] output = outtext.split("\\t");
				list.add(output[0]);
			}
			for(int i=0;i<list.size();i++){
				System.out.println(list.get(i));
				File fXmlFile = new File("/mnt/info_xml/"+list.get(i));
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(fXmlFile);
				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("newsitem");

				for (int temp = 0; temp < nList.getLength(); temp++) {

					Node nNode = nList.item(temp);
					if (nNode.getNodeType() == Node.ELEMENT_NODE) {

						Element eElement = (Element) nNode;

						News tempNews = new News();
						tempNews.setTitle(eElement.getElementsByTagName("title").item(0).getTextContent());
						tempNews.setHeadline(eElement.getElementsByTagName("headline").item(0).getTextContent());
						tempNews.setText(eElement.getElementsByTagName("text").item(0).getTextContent());

						String outfile = list.get(i).replace("xml", "txt");
						PrintWriter writer = new PrintWriter("text/"+outfile);
						PrintWriter finalout = new PrintWriter(new BufferedWriter(new FileWriter("outputfiles.txt", true)));
						writer.println(tempNews.getHeadline()+"\n");
						writer.println(tempNews.getText());
						finalout.println(outfile+";"+tempNews.getHeadline());
						writer.close();
						finalout.close();
					}
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
}
