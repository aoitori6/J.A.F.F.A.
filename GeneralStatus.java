public enum GeneralStatus {

}

enum LoginStatus {
    LOGIN_REQUEST, LOGIN_SUCCESS, LOGIN_FAIL,
};

enum RegisterStatus {
    REGISTER_REQUEST, REGISTER_SUCCESS, REGISTER_FAIL,
};

enum DownloadStatus {
    DOWNLOAD_REQUEST, DOWNLOAD_START, DOWNLOAD_SUCCESS, DOWNLOAD_FAIL,
};

enum UploadStatus {
    UPLOAD_REQUEST, UPLOAD_START, UPLOAD_SUCCESS, UPLOAD_FAIL,
};

enum LocateServerStatus{
    GET_SERVER, SERVER_FOUND, SERVER_NOT_FOUND
};