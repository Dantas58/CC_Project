import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.zip.CRC32;
import java.io.*;

public class FS_Node {

    // private final int TCP_Port = 9091;
    private String directory;

    private String server_address;
    private int server_port;

    private Map<String, List<FileBlock>> files;

    private ObjectInputStream in;
    private ObjectOutputStream out;

    private Socket tcp_socket;
    private DatagramSocket udp_socket;

    public FS_Node(int server_port, String server_address, String directory) {

        this.server_address = server_address;
        this.server_port = server_port;
        this.directory = directory;
        // tcp_socket = new Socket(server_address, server_port);
        // this.node_address = InetAddress.getLocalHost().getHostAddress(); // + ":" +
        // String.valueOf(TCP_Port);
    }

    public String getAddress() throws IOException {

        String address = InetAddress.getLocalHost().getHostAddress();// + ":" + String.valueOf(TCP_Port);
        return address;
    }

    public void register() throws IOException {

        String address = getAddress();

        this.files = readFilesToMap(directory);
        Track_Packet packet = new Track_Packet("REGISTER", address, generateBlockIdsMap(files));

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();

        System.out.println("Node has been Registered in FS Tracker;\n");
    }

    public void update() throws IOException {

        String address = getAddress();
        this.files = readFilesToMap(directory);

        Track_Packet packet = new Track_Packet("UPDATE", address, generateBlockIdsMap(files));

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }

    public void get(String file_name) throws IOException {

        String address = getAddress();

        Map<String, List<Integer>> files_just_name = new HashMap<>();
        files_just_name.put(file_name, new ArrayList<Integer>());
        Track_Packet packet = new Track_Packet("GET", address, files_just_name);

        byte[] packet_ready = packet.packUp();
        out.writeObject(packet_ready);
        out.flush();
    }

    public void exit() throws IOException {
        String address = getAddress();

        // Use a placeholder as there will be no need to send actual file information
        Map<String, List<Integer>> placeHolder = new HashMap<>();
        placeHolder.put("placeHolder", new ArrayList<>());

        Track_Packet packet = new Track_Packet("EXIT", address, placeHolder);
        byte[] packetReady = packet.packUp();
        
        out.writeObject(packetReady);
        out.flush();

        tcp_socket.close();

        if (udp_socket != null && !udp_socket.isClosed()) {
            udp_socket.close();
        }

        in.close();
        out.close();
    }

    public class FileBlock {

        private Integer Id;
        private byte[] data;

        public FileBlock(Integer blockId, byte[] data) {
            this.Id = blockId;
            this.data = data;
        }

        public Integer getBlockId() {
            return Id;
        }

        public byte[] getData() {
            return data;
        }
    }

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

    public long calcChecksum(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }

    public void setupPeer() {

        try {
            udp_socket = new DatagramSocket(9090);

            System.out.println("FS_Transfer Protocol: Listening on Port 9090;");
            
            new Thread(() -> {
                while (true) {
                    byte[] buffer = new byte[2048];
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    try {
                        udp_socket.receive(packet);
                        /* 
                        //Transfer_packet receivedPacket = Transfer_packet.deserialize(buffer);
                        if (receivedPacket.verifyChecksum(receivedPacket.getChecksum())) {
                            storeReceivedBlock(receivedPacket);
                        } else {
                            System.err.println("Checksum mismatch for block " + receivedPacket.getBlockNumber());
                        }
                        */
                    } catch (IOException e) {
                        System.err.println("Error receiving UDP packet: " + e.getMessage());
                    }
                }
            }).start();
            
        } catch (SocketException e) {
            System.err.println("Erro ao abrir o socket UDP: " + e.getMessage());
        }
    }

    public void setupTrackerConnection() throws IOException, ClassNotFoundException {

        tcp_socket = new Socket(server_address, server_port);
        out = new ObjectOutputStream(tcp_socket.getOutputStream());
        in = new ObjectInputStream(tcp_socket.getInputStream());

        System.out.println(
                "FS Track Protocol connection established with server " + server_address + " on port " + server_port);

        register();
    }

    public void commandHandler() throws IOException, ClassNotFoundException {

        Scanner scanner = new Scanner(System.in);

        while (true) {

            System.out.println("Waiting for a command:");
            String command = (scanner.nextLine()).toUpperCase();

            switch (command) {

                case "UPDATE":

                    update();
                    break;

                case "GET":

                    System.out.println("Choose file to locate:");
                    String file_name = scanner.nextLine();
                    get(file_name);

                    byte[] received_packet = (byte[]) in.readObject();
                    Track_Packet final_packet = Track_Packet.unpack(received_packet);

                    if (final_packet.getFiles().isEmpty())
                        System.out.println("Specified file could not be found in any registered Node;");

                    else {
                        for (String key : final_packet.getFiles().keySet()) {
                            System.out.println(key + ": " + final_packet.getFiles().get(key) + " blocks");
                        }
                    }

                    break;

                case "TRANSFER":

                    System.out.println("Choose file to transer: ");
                    String name = scanner.nextLine();
                    //transfer(name);

                case "EXIT":

                    exit();
                    // tcp_socket.close();
                    return;

                default:

                    System.out.println("Error: command is not valid;");

            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        if (args.length < 3) {
            System.out.println("Not enough arguments");
            return;
        }

        String directory = args[0];
        String server_address = args[1];
        int server_port = Integer.parseInt(args[2]);

        FS_Node node = new FS_Node(server_port, server_address, directory);

        node.setupTrackerConnection();
        node.setupPeer();
        node.commandHandler();
    }
}
