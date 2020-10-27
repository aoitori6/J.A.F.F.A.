package misc;

public class FileInfo {
    private String name;
    private String code;
    private String uploader;
    private Integer downloadsRemaining;
    private String deletionTimestamp;
    private long size;

    public FileInfo(String name, String code, long size, String uploader, Integer downloadsRemaining,
            String deletionTimestamp) {
        this.name = name;
        this.code = code;
        this.size = size;
        this.uploader = uploader;
        this.downloadsRemaining = downloadsRemaining;
        this.deletionTimestamp = deletionTimestamp;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return this.name;
    }

    public String getCode() {
        return this.code;
    }

    public long getSize() {
        return this.size;
    }

    public String getUploader() {
        return this.uploader;
    }

    public Integer getDownloadsRemaining() {
        return this.downloadsRemaining;
    }

    public String getDeletionTimestamp() {
        return this.deletionTimestamp;
    }
}
