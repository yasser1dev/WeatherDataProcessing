import ExtractData.ExtractData;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.log4j.BasicConfigurator;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLoggerManager;

import java.io.*;
import java.net.URL;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class MainClass {
    public static int minAnnee;
    public static int maxAnnee;
    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        System.setProperty("hadoop.name.dir","C:\\hadoop");
        Configuration configuration=new Configuration();
        FileSystem fs=FileSystem.get(configuration);
        System.out.println("Entrer  année min");
        Scanner sc = new Scanner(System.in);
        minAnnee = sc.nextInt();

        System.out.println("Entrer  année max");
        Scanner sc1 = new Scanner(System.in);
        maxAnnee = sc1.nextInt();

        ExtractData.minAnnee=minAnnee;
        ExtractData.maxAnnee=maxAnnee;
        ExtractData.downloadData();






        /*String csvFile = "C:\\Users\\Lenovo\\Desktop\\02907099999.csv";
        String line = "";
        String cvsSplitBy = ",";

        try  {
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                // use comma as separator
                //String[] country = line.split(cvsSplitBy);
                System.out.println(line);
                System.out.println("==================");


            }

        } catch (IOException e) {
            e.printStackTrace();
        }*/

    }

}
