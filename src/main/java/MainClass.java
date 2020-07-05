import ExtractData.ExtractData;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapreduce.Job;
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

        File allYearsData=new File("src/main/resources/allYearsFileData.csv");
        FileWriter fw=new FileWriter(allYearsData,true);
        BufferedWriter br = new BufferedWriter(fw);
        PrintWriter pr = new PrintWriter(br);

        String line = "";
        File[] files=new File(ExtractData.uncompressedFiles).listFiles();
        for(File file:files){
            File[] subFiles=new File(file.getAbsolutePath()).listFiles();
            System.out.println(file.getName());
            for(File subFile:subFiles){
                System.out.println("--------------------> "+subFile.getName());
                try  {
                    BufferedReader brSubFile = new BufferedReader(new FileReader(subFile));
                    while ((line = brSubFile.readLine()) != null) {
                        pr.println(line);
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



    }

}
