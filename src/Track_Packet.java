import java.nio.ByteBuffer;
import java.util.*;
import java.io.*;

public class Track_Packet {
    private String command;
    private String nodeAddress;
    private Map<String, List<Integer>> files;

    public Track_Packet(String command, String nodeAddress, Map<String, List<Integer>> files) {
        this.command = command;
        this.nodeAddress = nodeAddress;
        this.files = files;
    }

    public String getCommand(){
        return command;
    }

    public String getNodeAddress(){
        return nodeAddress;
    }

    public Map<String, List<Integer>> getFiles(){
        return files;
    }

    public void setCommand(String command){
        this.command = command;
    }

    public void setNodeAddress(String nodeAddress){
        this.nodeAddress = nodeAddress;
    }

    public void setFiles(Map<String, List<Integer>> files){
        this.files = files;
    }

    public byte[] packUp() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    
        // Pack command
        byte[] commandBytes = command.getBytes();
        int commandSize = commandBytes.length;
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(commandSize).array());
        byteArrayOutputStream.write(commandBytes);
    
        // Pack node address
        byte[] nodeAddressBytes = nodeAddress.getBytes();
        int nodeAddressSize = nodeAddressBytes.length;
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(nodeAddressSize).array());
        byteArrayOutputStream.write(nodeAddressBytes);
    
        // Pack file count
        int fileCount = files.size();
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(fileCount).array());
    
        // Pack files
        for (Map.Entry<String, List<Integer>> entry : files.entrySet()) {
            String fileName = entry.getKey();
            List<Integer> blockIds = entry.getValue();
    
            // Pack file name
            byte[] fileNameBytes = fileName.getBytes();
            int fileNameSize = fileNameBytes.length;
            byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(fileNameSize).array());
            byteArrayOutputStream.write(fileNameBytes);
    
            // Pack block IDs count
            int blockIdsCount = blockIds.size();
            byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(blockIdsCount).array());
    
            // Pack block IDs
            for (int blockId : blockIds) {
                byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(blockId).array());
            }
        }
    
        return byteArrayOutputStream.toByteArray();
    }
        
    
    public static Track_Packet unpack(byte[] serializedData) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedData);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    
        int commandSize = dataInputStream.readInt();
        byte[] commandBytes = new byte[commandSize];
        dataInputStream.readFully(commandBytes);
        String command = new String(commandBytes);
    
        int nodeAddressSize = dataInputStream.readInt();
        byte[] nodeAddressBytes = new byte[nodeAddressSize];
        dataInputStream.readFully(nodeAddressBytes);
        String nodeAddress = new String(nodeAddressBytes);
    
        int fileCount = dataInputStream.readInt();
        
        Map<String, List<Integer>> files = new HashMap<>();
    
        for (int i = 0; i < fileCount; i++) {
            int fileNameSize = dataInputStream.readInt();
            byte[] fileNameBytes = new byte[fileNameSize];
            dataInputStream.readFully(fileNameBytes);
            String fileName = new String(fileNameBytes);
    
            int blockIdsCount = dataInputStream.readInt();
            List<Integer> blockIds = new ArrayList<>();
    
            for (int j = 0; j < blockIdsCount; j++) {
                int blockId = dataInputStream.readInt();
                blockIds.add(blockId);
            }
    
            files.put(fileName, blockIds);
        }

        return new Track_Packet(command, nodeAddress, files);
    }
}
