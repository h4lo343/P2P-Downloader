package comp90015.idxsrv.message;

/**
 * @author XIANGNAN ZHOU_1243072
 * @date 2022/9/5 22:34
 */

@JsonSerializable
public class Goodbye extends Message{

    @JsonElement
    public String message;

    public Goodbye() {

    }
    public Goodbye(String message) {
        this.message = message;
    }
}
