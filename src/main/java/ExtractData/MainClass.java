package ExtractData;

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


        File resultPath;
        URL url;
        String uncompressedFiles="C:\\Users\\Lenovo\\Desktop\\weatherDataUncompressed";
        File destFile = new File(uncompressedFiles);
        if (!destFile.exists()) {
            if (destFile.mkdir()) {
                System.out.println("Directory is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }

        for(int i=minAnnee;i<=maxAnnee;i++){
            resultPath=new File("C:\\Users\\Lenovo\\Desktop\\weatherData\\"+i+".tar.gz");
            url=new URL("https://www.ncei.noaa.gov/data/global-hourly/archive/csv/"+i+".tar.gz");
            System.out.println("Downloading file : "+i+".tar.gz");
            FileUtils.copyURLToFile(url,resultPath);
            unTarFile(resultPath, destFile);
        }


    }
    private static void unTarFile(File tarFile, File destFile) {
        File yearFolder = new File(destFile.getPath()+"\\"+tarFile.getName().split("[.]")[0]);
        if (!yearFolder.exists()) {
            if (yearFolder.mkdir()) {
                System.out.println("Year dir is created!");
            } else {
                System.out.println("Failed to create directory!");
            }
        }

        final TarGZipUnArchiver ua = new TarGZipUnArchiver();
        ConsoleLoggerManager manager = new ConsoleLoggerManager();
        manager.initialize();

        ua.enableLogging(manager.getLoggerForComponent("bla"));
        ua.setSourceFile(tarFile);

        ua.setDestDirectory(yearFolder);
        ua.extract();
    }
}
