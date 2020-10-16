import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

final public class UserClient {

    // Private Constructor to prevent Instantiation 
    private UserClient() {
    }

    // Console Input
    final static Scanner conInput = new Scanner(System.in);

    // Path to User's Home
    final static String HOME = System.getProperty("user.home");

    // Client Object tied with User Interface
    static Client client= null;

    private static void downloadFile(){

            /*TODO: Check valid File Paths, Check existing Files, 
            Check File Path for Linux*/

        // Get File Code
        System.out.println("Enter File Code");
        String code = conInput.nextLine();

        // Get File Save Path
        System.out.println("Enter File Save Path (Absolute Path) (Currently Windows Only)");
        System.out.println("Default Save Path is User's Home Directory");
        String savePath = conInput.nextLine();

        if(savePath.equals(""))
            savePath = HOME;

        if(client.downloadFile(code, Paths.get(savePath)) == DownloadStatus.DOWNLOAD_SUCCESS)
            System.out.println("File Downloaded Succesfully");
        else System.out.println("ERROR. Failed to Download File");
    }

    // Main User Interface
    public static void main(String[] args) {
        byte choice;
        
        // Getting AuthServer Address
        System.out.println("Enter Address of Authentication Server");
        String address = conInput.nextLine();

        System.out.println("Enter Port of Authentication Server");
        int port = conInput.nextInt();
        conInput.nextLine();

            // TODO: Check validity of Port and Address

        // Connecting to AuthServer
        client = new Client(address, port);
        if(client == null)
            return;

        // Getting Account Details
        System.out.println("Enter Username");
        String username = conInput.nextLine();

        System.out.println("Enter Password");
        String password = conInput.nextLine();

        // Authenticating Client
            // TODO: Handle Bad Logins
        if(client.logIn(username, password) == false)
            return;

        // Main Menu
        menu:
        while(true){
            System.out.println("1. Download File");
            System.out.println("2. Upload File");
            System.out.println("3. Delete File");
            System.out.println("5. Logout");
            System.out.println("Enter your Choice");
            choice = conInput.nextByte();
            conInput.nextLine();
            
            switch(choice){
                case 1: downloadFile();
                        break;

                case 5: break menu;
                default: System.out.println("ERROR. Invalid Choice");
            }
        }

        return;

    }
}
