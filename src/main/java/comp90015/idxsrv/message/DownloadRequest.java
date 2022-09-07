package comp90015.idxsrv.message;

/**
 * @author XIANGNAN ZHOU_1243072
 * @date 2022/9/6 14:21
 */

@JsonSerializable
public class DownloadRequest extends Message {

    @JsonElement
    public String filename;

    @JsonElement
    public String fileMD5;

    public DownloadRequest() {

    }

    public DownloadRequest(String filename, String fileMD5) {
        this.filename = filename;
        this.fileMD5 = fileMD5;
    }
}
