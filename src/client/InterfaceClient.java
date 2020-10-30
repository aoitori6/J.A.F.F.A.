package client;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import statuscodes.*;

final public class InterfaceClient {

    // Private Constructor to prevent Instantiation
    private InterfaceClient() {
    }

    // Console Input
    final static Scanner conInput = new Scanner(System.in);

    // Path to User's Home
    final static String HOME = System.getProperty("user.home");

    // Client Object tied with User Interface
    static Client client = null;

    private static boolean register() {
        String username, password;
        while (true) {
            System.out.println("Enter Username");
            username = conInput.nextLine();

            Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
            Matcher matcher = pattern.matcher(username.trim());

            if (username.trim().length() < 3 || username.trim().length() > 30) {
                System.out.println("Username should be between 3 and 30 characters long");
            } else if (matcher.find()) {
                System.out.println("Username should consist of only alphabets and numbers");
            } else
                break;
        }

        while (true) {
            System.out.println("Enter Password");
            password = conInput.nextLine();

            Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
            Matcher matcher = pattern.matcher(password.trim());

            if (password.trim().length() < 6) {
                System.out.println("Password should be atleast 6 characters long");
            } else if (matcher.find()) {
                System.out.println("Password should consist of only alphabets and numbers");
            } else
                break;
        }
        System.out.println("Confirm Password");
        String confirmPassword = conInput.nextLine();

        if (!password.equals(confirmPassword)) {
            System.out.println("Passwords do not match");
            return false;
        }

        return client.register(username.trim(), password.trim());
    }

    private static boolean logIn() {

        System.out.println("Enter Username");
        String username = conInput.nextLine();

        System.out.println("Enter Password");
        String password = conInput.nextLine();

        return client.logIn(username, password);
    }

    private static void downloadFile() {

        /*
         * TODO: Check valid File Paths, Check existing Files, Check File Path for Linux
         */

        // Get File Code
        System.out.println("Enter File Code");
        String code = conInput.nextLine();

        // Get File Save Path
        System.out.println("Enter File Save Path (Absolute Path) (Currently Windows Only)");
        System.out.println("Default Save Path is User's Home Directory");
        String savePath = conInput.nextLine();

        if (savePath.equals(" "))
            savePath = HOME;

        System.out.println("Querying Server");
        if (client.downloadFile(code, Paths.get(savePath)) == DownloadStatus.DOWNLOAD_SUCCESS)
            System.out.println("File Downloaded Successfully");
        else
            System.out.println("ERROR. Failed to Download File");
    }

    private static void uploadFile() {

        String timestamp;
        String downloads;
        Integer downloadCap;

        // Get File Path
        System.out.println("Enter Path to the File (Absolute Path) (Currently Windows Only)");
        String filePath = conInput.nextLine();

        // Check If File Exists
        if (Files.exists(Paths.get(filePath)) == false) {
            System.out.println("ERROR. Invalid File Path!");
            return;
        }

        // Get Download Cap (if any)
        while (true) {
            System.out.println("Enter Download Cap (or press enter for no download cap)");
            downloads = conInput.nextLine();
            if (downloads.equals("")) {
                downloadCap = null;
                break;
            } else
                downloadCap = Integer.parseInt(downloads);
            try {
                if (downloadCap.intValue() <= 0) {
                    System.out.println("Download Cap must be greater than 0 (or press enter for no download cap)");
                } else
                    break;
            } catch (NumberFormatException e) {
                System.out.println("Invalid input");
            }
        }

        // Get Deletion Timestamp (if any)
        System.out.println("Enter 1 to set a deletion timestamp or 2 for no deletion timestamp");
        int choice = conInput.nextInt();
        if (choice == 1) {
            System.out.println("Enter the no.of days: ");
            int days = conInput.nextInt();
            System.out.println("Enter the no.of hours: ");
            int hours = conInput.nextInt();
            System.out.println("Enter the no.of minutes: ");
            int minutes = conInput.nextInt();

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            LocalDateTime ldt = LocalDateTime.now(ZoneId.of("UTC"));
            long totalMinutes = days * 24 * 60 + hours * 60 + minutes;

            timestamp = ldt.plus(totalMinutes, ChronoUnit.MINUTES).format(formatter);
        } else
            timestamp = null;
        System.out.println("Querying Server");

        // Reciving The Code, Code Will Be null If Uploading Failed
        String code = client.uploadFile(Paths.get(filePath), downloadCap, timestamp);
        if (code != null) {
            System.out.println("File Uploaded Successfully");
            System.out.println("Code: " + code);
        } else
            System.out.println("ERROR. Failed to Upload File");
    }

    private static void deleteFile() {

        // Get File Code
        System.out.println("Enter File Code");
        String code = conInput.nextLine();

        if (client.deleteFileWrapper(code))
            System.out.println("File Deleted Successfully");
        else
            System.out.println("ERROR. Failed to Delete File");
    }

    // Main User Interface
    public static void main(String[] args) {
        byte choice;

        // Getting AuthServer Address
        /*
         * System.out.println("Enter Address of Authentication Server"); String address
         * = conInput.nextLine();
         * 
         * System.out.println("Enter Port of Authentication Server"); int port =
         * conInput.nextInt(); conInput.nextLine();
         */
        String address = "localhost";
        int port = 9000;

        // Connecting to AuthServer
        client = new Client(address, port);
        if (client == null)
            return;

        program: while (true) {
            // Authenticating Client
            // TODO: Handle Bad Logins
            auth: while (true) {
                System.out.println("1. Register");
                System.out.println("2. Login");
                System.out.println("3. Exit");
                choice = conInput.nextByte();
                conInput.nextLine();

                switch (choice) {
                    case 1:
                        if (register())
                            break auth;
                        break;
                    case 2:
                        if (logIn())
                            break auth;
                        break;
                    case 3:
                        break program;
                    default:
                        System.out.println("ERROR. Invalid Choice");
                }
            }

            // Main Menu
            menu: while (true) {
                System.out.println("1. Download File");
                System.out.println("2. Upload File");
                System.out.println("3. Delete File");
                System.out.println("4. Logout");
                System.out.println("Enter your Choice");
                choice = conInput.nextByte();
                conInput.nextLine();

                switch (choice) {
                    case 1:
                        downloadFile();
                        break;
                    case 2:
                        uploadFile();
                        break;
                    case 3:
                        deleteFile();
                        break;
                    case 4:
                        if (client.logout()) {
                            System.out.println("Logged Out");
                            break menu;
                        } else {
                            System.out.println("Logging Out Failed");
                            break;
                        }
                    default:
                        System.out.println("ERROR. Invalid Choice");
                }
            }
        }
        return;
    }
}
