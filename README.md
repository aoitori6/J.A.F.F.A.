# Softablitz_Share-Now

[X]Basic Features:
  [X]User Features:
[X] 1. A user should be able to register login and logout to the server.
[X] 2. Every user should be able to upload and download files.
[X] 3. File uploaded must be transferred to the server along with all the relevant details like owner, name, size, type.
[X] 4. Uploader should be able to tell how many times the given file can be downloaded. After that file is downloaded the mentioned number of times it should be deleted from the server.
[X] 5. Every uploaded file must have a code generated which will be shared.
[X]6. Every user should be able to download files from the server with the code given to him.

  [X] Admin Features:
[X]1. Separate login for Admin.
[X]2. Admin should be able to see how many files have been shared by all the users along with all file info and number of downloads left.
[X] 3. Admin should be able to remove any file on ground of violation of code ofconduct.

[X]Advanced Features:
[X] 1. Uploader can set the time for which the file should be available for download, after that time passes the file should be deleted from the server
[X] 2. Uploader can revoke a file using its identifier. If he does so then that file identifier can't be used to download a file anymore or file gets deleted from the
server
[X] 3. There can be multiple storage servers to store files with a proper load balancing based on some parameter.
[X] 4. File can be stored to multiple servers so that if a storage server goes down the user is still able to download the file.
