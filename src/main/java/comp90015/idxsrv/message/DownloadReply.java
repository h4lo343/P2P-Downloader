package comp90015.idxsrv.message;

/**
 * @author XIANGNAN ZHOU_1243072
 * @date 2022/9/6 14:21
 */
@JsonSerializable
public class DownloadReply extends Message {

    @JsonElement
    public Boolean success;

    public DownloadReply() {

    }

    public DownloadReply(boolean success) {
        this.success = success;
    }
}
