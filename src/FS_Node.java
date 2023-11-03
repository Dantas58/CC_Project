import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.io.*;


public class FS_Node {
    
    private int TCP_Port;

    private String server_address;
    private int server_port;

    private ObjectInputStream in;
    private ObjectOutputStream out;

    public FS_Node(){
        
        this.TCP_Port = 9091;
        this.server_address = "localhost";
        this.server_port = 9090;
    }


    public void FSTrackerConnection() throws IOException, ClassNotFoundException{

        Socket socket = new Socket(server_address, server_port);
        
        out = new ObjectOutputStream(socket.getOutputStream());
        in = new ObjectInputStream(socket.getInputStream()); 

        System.out.println("FS Trak Protocol connection established with server " + server_address + " on port " + server_port);

        register();
        System.out.println("Node has been Registered in FS Tracker;\n");

        Scanner scanner = new Scanner(System.in);

        while(true){

            System.out.println("Waiting for a command:");
            String command = (scanner.nextLine()).toUpperCase();

            switch(command){
                
                case "UPDATE":

                    update();
                    break;

                case "GET":

                    System.out.println("Choose file to locate:");
                    String file_name = scanner.nextLine();
                    get(file_name);

                    byte[] received_packet = (byte[]) in.readObject();
                    Protocol final_packet = Protocol.unpack(received_packet);

                    System.out.println(final_packet.getCommand());

                    break;

                default:

                    System.out.println("Error: command is not valid;");

            }
        }
    }


    public void register() throws IOException{

        String address = InetAddress.getLocalHost().getHostAddress() + ":" + String.valueOf(TCP_Port);
        Map<String, List<byte[]>> files = readFilesToMap("/home/zao/Desktop/CC_Project/files");   //tenho de mudar a função
        Protocol packet = new Protocol("REGISTER", address, files);

        byte[] packet_ready = packet.packUp();
        //int packet_size = packet_ready.length;
        //out.writeObject(packet_size);
        out.writeObject(packet_ready);
        out.flush();
    }

    public void update() throws IOException{


        String address = InetAddress.getLocalHost().getHostAddress() + ":" + String.valueOf(TCP_Port);
        Map<String, List<byte[]>> updated_files = readFilesToMap("/home/zao/Desktop/CC_Project/files");

        Protocol packet = new Protocol("UPDATE", address, updated_files);

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }

    public void get(String file_name) throws IOException{

        String address = InetAddress.getLocalHost().getHostAddress() + ":" + String.valueOf(TCP_Port);
        Map<String, List<byte[]>> files_just_name = new HashMap<>();
        //List<byte[]> lista = new ArrayList<>();
        files_just_name.put(file_name, new ArrayList<>()); 
        Protocol packet = new Protocol("GET", address, files_just_name); //pensar como mandar a packet para get

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }


    public Map<String, List<byte[]>> readFilesToMap(String directoryPath) {

        File directory = new File(directoryPath);

        Map<String, List<byte[]>> fileMap = new HashMap<>();

        File[] files = directory.listFiles();
        
        if (files != null) {

            for (File file : files) {
                if (file.isFile()) {
                    String fileName = file.getName();
                    List<byte[]> fileContent = new ArrayList<>();

                    try (FileInputStream fileInputStream = new FileInputStream(file)) {
                        int bufferSize = 1024; // You can adjust the buffer size as needed
                        byte[] buffer = new byte[bufferSize];
                        int bytesRead;

                        while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                            byte[] data = new byte[bytesRead];
                            System.arraycopy(buffer, 0, data, 0, bytesRead);
                            fileContent.add(data);
                        }

                        fileMap.put(fileName, fileContent);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return fileMap;
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {

        //String directory = args[0];
        //String server_address = args[1];
        //int server_port = Integer.parseInt(args[2]);
        //FS_Node node = new FS_Node(directory, server_address, server_port); // para depois

        FS_Node node = new FS_Node();
        node.FSTrackerConnection();
    }
}
