package comp90015.idxsrv.message;

/**
 * @author XIANGNAN ZHOU_1243072
 * @date 2022/9/5 22:36
 */
@JsonSerializable
public class BlockReply extends  Message{

    @JsonElement
    public String filename;

    @JsonElement
    public String fileMd5;

    @JsonElement
    public Integer blockIdx;

    @JsonElement
    public String bytes;

    public BlockReply() {

    }

    public BlockReply(String filename, String fileMd5, Integer blockIdx, String bytes) {
        this.filename = filename;
        this.fileMd5 = fileMd5;
        this.blockIdx = blockIdx;
        this.bytes = bytes;
    }
}
