package com.shmztko.batch.framework;

import org.easybatch.core.reader.RecordReader;
import org.easybatch.core.reader.RecordReaderClosingException;
import org.easybatch.core.reader.RecordReaderOpeningException;
import org.easybatch.core.reader.RecordReadingException;
import org.easybatch.core.record.Header;
import org.easybatch.core.record.Record;
import org.easybatch.core.record.StringRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.util.*;

import static java.lang.String.format;
import static org.easybatch.core.util.Utils.checkArgument;

public class FileLineReader implements RecordReader {

    private Logger logger = LoggerFactory.getLogger(FileLineReader.class);

    private File directory;
    private String charsetName;
    private List<File> files;


    private long currentRecordNumber;

    private List<Scanner> scanners;
    private ListIterator<Scanner> scannerIterator;
    private Scanner currentScanner;


    public FileLineReader(final String directoryName) throws FileNotFoundException {
        this(new File(directoryName), Charset.defaultCharset().name());
    }

    public FileLineReader(final File directory) throws FileNotFoundException {
        this(directory, Charset.defaultCharset().name());
    }

    public FileLineReader(final File directory, final String charsetName) throws FileNotFoundException {
        this.directory = directory;
        this.charsetName = charsetName;


    }

    @Override
    public void open() throws RecordReaderOpeningException {
        checkDirectory();
        currentRecordNumber = 0;
        this.files = getFiles(directory);

        scanners = new ArrayList<>();
        for (File file : this.files) {
            try {
                scanners.add(new Scanner(new FileInputStream(file), charsetName));
            } catch (FileNotFoundException e) {
                throw new RecordReaderOpeningException("Unable to find file " + file.getName(), e);
            }
        }
        if (scanners.size() > 0) {
            scannerIterator = scanners.listIterator();
            currentScanner = scannerIterator.next();
        } else {
            throw new RuntimeException("No files in directory " + directory.getName());
        }
    }

    @Override
    public boolean hasNextRecord() {
        return currentScanner.hasNext() || scannerIterator.hasNext();
    }

    @Override
    public Record readNextRecord() throws RecordReadingException {
        Header header = new Header(++currentRecordNumber, getDataSourceName(), new Date());

        if (currentScanner.hasNext()) {
            return new StringRecord(header, currentScanner.nextLine());

        } else if (scannerIterator.hasNext()) {
            currentScanner = scannerIterator.next();
            return new StringRecord(header, currentScanner.nextLine());

        } else {
            // Should not come here. because hasNextRecord should be called first.
            throw new RecordReadingException("Unexpected error occurred. No data source found");
        }
    }

    @Override
    public Long getTotalRecords() {
        long totalRecords = 0;

        for (File file : files) {
            Scanner recordCounterScanner = null;
            try {
                recordCounterScanner = new Scanner(new FileInputStream(file), charsetName);
                while (recordCounterScanner.hasNextLine()) {
                    totalRecords++;
                    recordCounterScanner.nextLine();
                }
            } catch (FileNotFoundException e) {
                logger.info("Unable to calculate total records number", e);
                return null;
            } finally {
                if (recordCounterScanner != null) {
                    recordCounterScanner.close();
                }
            }
        }
        return totalRecords;
    }

    @Override
    public String getDataSourceName() {
        return this.directory.getAbsolutePath() + this.currentScanner.toString();
    }

    @Override
    public void close() throws RecordReaderClosingException {
        for (Scanner scanner : scanners) {
            scanner.close();
        }
    }

    private List<File> getFiles(final File directory) {
        List<File> files = new ArrayList<>();
        File[] filesList = directory.listFiles();
        if (filesList != null) {
            for (File file : filesList) {
                if (file.isFile()) {
                    files.add(file);
                } else {
                    files.addAll(getFiles(file));
                }
            }
        }
        return files;
    }

    private void checkDirectory() {
        checkArgument(directory.exists(), format("Directory %s does not exist.", directory.getAbsolutePath()));
        checkArgument(directory.isDirectory(), format("%s is not a directory.", directory.getAbsolutePath()));
        checkArgument(directory.canRead(), format("Unable to read files from directory %s. Permission denied.", directory.getAbsolutePath()));
    }
}
