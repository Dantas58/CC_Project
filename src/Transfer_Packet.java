import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class Transfer_Packet {
    
    private String file_name;
    private Integer block_id;
    private byte[] block_data;
    private long checksum;

    public Transfer_Packet(String file_name, Integer block_id, byte[] data, long checksum) {
        this.file_name = file_name;
        this.block_id = block_id;
        this.block_data = data;
        this.checksum = checksum;
    }

    public String getFileName() {
        return file_name;
    }

    public Integer getBlockId() {
        return block_id;
    }

    public byte[] getBlockData() {
        return block_data;
    }

    public long getChecksum() {
        return checksum;
    }


    public byte[] packUpTransfer() throws IOException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    
        byte[] fileNameBytes = file_name.getBytes();
        int fileNameSize = fileNameBytes.length;
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(fileNameSize).array());
        byteArrayOutputStream.write(fileNameBytes);

        // Pack block ID
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(block_id).array());

        // Pack data
        byte[] data = this.block_data;
        int dataSize = data.length;
        byteArrayOutputStream.write(ByteBuffer.allocate(4).putInt(dataSize).array());
        byteArrayOutputStream.write(data);

        // Pack checksum
        byteArrayOutputStream.write(ByteBuffer.allocate(8).putLong(checksum).array());
    
        return byteArrayOutputStream.toByteArray();
    }

    public static Transfer_Packet unpackTransfer (byte[] transfer_packet) throws IOException {

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(transfer_packet);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);

        int fileNameSize = dataInputStream.readInt();
        byte[] fileNameBytes = new byte[fileNameSize];
        dataInputStream.readFully(fileNameBytes);
        String fileName = new String(fileNameBytes);

        int blockId = dataInputStream.readInt();

        int dataSize = dataInputStream.readInt();
        byte[] data = new byte[dataSize];
        dataInputStream.readFully(data);

        long checksum = dataInputStream.readLong();

        return new Transfer_Packet(fileName, blockId, data, checksum);
    }
}

