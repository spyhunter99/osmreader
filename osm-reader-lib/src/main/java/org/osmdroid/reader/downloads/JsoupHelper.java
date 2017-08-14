package org.osmdroid.reader.downloads;

import org.jsoup.Jsoup;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * created on 8/13/2017.
 *
 * @author Alex O'Ree
 */

public class JsoupHelper {
    public static List<String> getDownloadlinks(String url) throws IOException {

        List<String> urls = new ArrayList<String>();
        Document doc = Jsoup.connect(url).get();
        Elements links = doc.select("a[href]");

        for (Element link : links) {
            String href = link.attr("href");
            if (href.endsWith(".pbf")) {
                urls.add(href);
            }
        }
        return urls;
    }

    private static void print(String msg, Object... args) {
        System.out.println(String.format(msg, args));
    }


}

