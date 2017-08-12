package org.osmdroid.reader.readers;

import org.osmdroid.reader.DBUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import java.io.BufferedReader;
import java.io.File;
import java.sql.Connection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Set;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

/**
 * created on 8/12/2017.
 *
 * @author Alex O'Ree
 */

public class OsmSaxParser implements IOsmReader {

    private XMLReader reader;
    private OsmHandler handler;


    public OsmSaxParser() {

        try {
            SAXParserFactory _f = SAXParserFactory.newInstance();
            SAXParser _p = null;
            _p = _f.newSAXParser();

            reader = _p.getXMLReader();
            handler = new OsmHandler();
            reader.setContentHandler(handler);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }


    public String getParserName() {
        return "SAX";
    }

    @Override
    public void setOptions(Set<Short> options) {

    }


    private static class OsmHandler extends DefaultHandler {
        private String currentElement;
        private DateFormat dateFormat =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        @Override
        public void startDocument() throws SAXException {

        }

        @Override
        public void startElement(
            String aUri, String aLocalName,
            String aQName, Attributes aAttributes
        ) throws SAXException {
            currentElement = aQName;
            //TODO
            /*if ("entry".equals(aQName)) {
                tweets.addTweet(tweet = new Tweet());
            } else if ("content".equals(aQName)) {
                tweet.setContent(content = new Content());
                content.setType(aAttributes.getValue("type"));
            } else if ("author".equals(aQName)) {
                tweet.setAuthor(author = new Author());
            }*/
        }

        @Override
        public void endElement(
            String aUri, String aLocalName, String aQName
        ) throws SAXException {
            currentElement = null;
        }

        @Override
        public void characters(char[] aCh, int aStart, int aLength)
            throws SAXException {
            //TODO
            /*

            if ("published".equals(currentElement)) {
                try {
                    tweet.setPublished(dateFormat.parse(
                        new String(aCh, aStart, aLength))
                    );
                } catch (ParseException anExc) {
                    throw new SAXException(anExc);
                }
            } else if (
                ("title".equals(currentElement)) &&
                    (tweet != null)
                ) {
                tweet.setTitle(new String(aCh, aStart, aLength));
            } else if ("content".equals(currentElement)) {
                content.setValue(new String(aCh, aStart, aLength));
            } else if ("lang".equals(currentElement)) {
                tweet.setLanguage(new String(aCh, aStart, aLength));
            } else if ("name".equals(currentElement)) {
                author.setName(new String(aCh, aStart, aLength));
            } else if ("uri".equals(currentElement)) {
                author.setUri(new String(aCh, aStart, aLength));
            }*/
        }
    }

    public double getProgress() {
        return 0;
    }

    public void read(File path, Connection connection) throws Exception {
        BufferedReader bufferedReaderForBZ2File = DBUtils.getBufferedReaderForBZ2File(path);
        InputSource source = new InputSource(bufferedReaderForBZ2File);
        reader.parse(source);
        DBUtils.safeClose(bufferedReaderForBZ2File);
    }
}
