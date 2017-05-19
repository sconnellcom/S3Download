# S3Download
A Windows command line application to download/restore files from an AWS S3 Bucket written in C# .net

I'm not sure where I got the AWSS3Helper.cs file

# Usage
- Add your aws credentials to program.cs line 60
```C#
AWSS3Helper h = new AWSS3Helper("Your Access Key ID", "Your Secrety Key", "Your Bucket Name");
```
- Uncomment and update the method calls in the Program Main to specify what you want to download, where you want it
- Run the program
