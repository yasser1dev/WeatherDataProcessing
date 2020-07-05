package ExtractData;

import org.apache.commons.io.FileUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLoggerManager;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class ExtractData {
    public static int minAnnee;
    public static int maxAnnee;
    public static String uncompressedFiles="C:\\Users\\Lenovo\\Desktop\\weatherDataUncompressed";


    public static void downloadData() throws IOException {
        File resultPath;
        URL url;
        uncompressedFiles="C:\\Users\\Lenovo\\Desktop\\weatherDataUncompressed";
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
