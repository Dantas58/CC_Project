import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
import java.io.*;


public class FS_Node {
    
    private int TCP_Port;
    private String directory;

    private String server_address;
    private int server_port;

    private Map<String, List<FileBlock>> files;

    private ObjectInputStream in;
    private ObjectOutputStream out;

    //private  String node_address;

    public FS_Node(int server_port, String server_address, String directory){
        
        this.TCP_Port = 9091;;
        this.server_address = server_address;
        this.server_port = server_port;
        this.directory = directory;
        //this.node_address = InetAddress.getLocalHost().getHostAddress(); // + ":" + String.valueOf(TCP_Port);
    }


    public void FSTrackerConnection() throws IOException, ClassNotFoundException{

        Socket socket = new Socket(server_address, server_port);
        
        out = new ObjectOutputStream(socket.getOutputStream());
        in = new ObjectInputStream(socket.getInputStream()); 

        System.out.println("FS Track Protocol connection established with server " + server_address + " on port " + server_port);

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

                    if(final_packet.getFiles().isEmpty()) 
                        System.out.println("Specified file could not be found in any registered Node;");

                    else{
                        for (String key : final_packet.getFiles().keySet()) {
                            System.out.println(key + ": " + final_packet.getFiles().get(key) + " blocks");
                        }
                    }

                    break;
                
                case "EXIT":

                    exit();
                    socket.close();
                    return;

                default:

                    System.out.println("Error: command is not valid;");

            }
        }
    }


    public String getAddress() throws IOException{

        String address = InetAddress.getLocalHost().getHostAddress();// + ":" + String.valueOf(TCP_Port);
        return address;
    }

    public void register() throws IOException{

        String address = getAddress();

        this.files = readFilesToMap(directory);
        Protocol packet = new Protocol("REGISTER", address, generateBlockIdsMap(files));

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }

    public void update() throws IOException{

        String address = getAddress();
        this.files = readFilesToMap(directory);

        Protocol packet = new Protocol("UPDATE", address, generateBlockIdsMap(files));

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }

    public void get(String file_name) throws IOException{

        String address = getAddress();

        Map<String, List<Integer>> files_just_name = new HashMap<>();
        files_just_name.put(file_name, new ArrayList<Integer>()); 
        Protocol packet = new Protocol("GET", address, files_just_name); 

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }

    public void exit() throws IOException{

        String address = getAddress();

        // Use a placeholder as there will be no need to send actual file data
        Map<String, List<Integer>> place_holder = new HashMap<>();
        place_holder.put("placeHolder", new ArrayList<>());

        Protocol packet = new Protocol("EXIT", address, place_holder);
        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();

        in.close();
        out.close();
    }

    /* 
    public Map<String, Integer> readFilesToMap(String directoryPath){    }


        File directory = new File(directoryPath);
    
        Map<String, Integer> fileBlockCounts = new HashMap<>();
    
        File[] files = directory.listFiles();
    
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    String fileName = file.getName();
    
                    try (FileInputStream fileInputStream = new FileInputStream(file)) {
                        int blockCount = 0;
                        int bufferSize = 100; 
                        byte[] buffer = new byte[bufferSize];
                        int bytesRead;
    
                        while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                            blockCount++;fileName    }

                        }
    
                        fileBlockCounts.put(fileName, blockCount);
    
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    
        return fileBlockCounts;
    }
*/

    public Map<String, List<FileBlock>> readFilesToMap(String directoryPath) {
        File directory = new File(directoryPath);
        Map<String, List<FileBlock>> fileBlocksMap = new HashMap<>();

        File[] files = directory.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    String fileName = file.getName();

                    try (FileInputStream fileInputStream = new FileInputStream(file)) {
                        int blockId = 0;
                        int bufferSize = 100; 
                        byte[] buffer = new byte[bufferSize];
                        int bytesRead;

                        List<FileBlock> fileBlocks = new ArrayList<>();

                        while ((bytesRead = fileInputStream.read(buffer)) != -1) {
                            FileBlock fileBlock = new FileBlock(blockId, buffer.clone());
                            fileBlocks.add(fileBlock);
                            blockId++;
                        }

                        fileBlocksMap.put(fileName, fileBlocks);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        return fileBlocksMap;
    }

    public Map<String, List<Integer>> generateBlockIdsMap(Map<String, List<FileBlock>> fileBlocksMap) {
        Map<String, List<Integer>> blockIdsMap = new HashMap<>();

        for (Map.Entry<String, List<FileBlock>> entry : fileBlocksMap.entrySet()) {
            String fileName = entry.getKey();
            List<FileBlock> fileBlocks = entry.getValue();
            List<Integer> blockIds = new ArrayList<>();

            for (FileBlock fileBlock : fileBlocks) {
                blockIds.add(fileBlock.getBlockId());
            }

            blockIdsMap.put(fileName, blockIds);
        }

        return blockIdsMap;
    }

    public class FileBlock {
        private Integer blockId;
        private byte[] data;
    
        public FileBlock(Integer blockId, byte[] data) {
            this.blockId = blockId;
            this.data = data;
        }
    
        public Integer getBlockId() {
            return blockId;
        }
    
        public byte[] getData() {
            return data;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        if(args.length < 3){
            System.out.println("Not enough arguments");
            return;
        }

        String directory = args[0];
        String server_address = args[1];
        int server_port = Integer.parseInt(args[2]);

        FS_Node node = new FS_Node(server_port, server_address, directory);
        node.FSTrackerConnection();
    }
}
