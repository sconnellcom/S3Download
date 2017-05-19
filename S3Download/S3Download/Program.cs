using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Core;

namespace S3Download
{
    static class Extensions
    {
        /// <summary>
        /// Get substring of specified number of characters on the right.
        /// </summary>
        public static string Right(this string value, int length)
        {
            if (String.IsNullOrEmpty(value)) return string.Empty;

            return value.Length <= length ? value : value.Substring(value.Length - length);
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            string[] textFileExtensions = (".asp;.htm;.html;.asa;.js;.vbs;.css;.aspx;.inc;.txt;.php;.xml;.cs;.ascx;.config;.dbml;.xsl;.master;.vb;.asax;.asmx;.ashx;.xsd;.skin;.dns;.cshtml").Split(';');

            //RetrieveFromS3("AWS Path to restore", "Local folder to save files to", textFileExtensions, true, false, false, false);
            //RetrieveFromS3("AWS Path to restore", "Local folder to save files to", null, true, false, false, false);
            //UnGzipFiles(AppDomain.CurrentDomain.BaseDirectory + "Local folder to save files to");

            Console.WriteLine("Press any key to exit");
            Console.ReadKey();

        }

        public static void UnGzipFiles(string path)
        {
            string[] files = Directory.GetFiles(path, "*.gz", SearchOption.AllDirectories);
            foreach (string file in files)
            {
                Console.WriteLine(file.Replace(path, ""));
                ExtractGZip(file, true);
            }
        }
        /// <summary>
        /// Retrieve server backups from AWS
        /// </summary>
        /// <param name="AWSPath">AWS Path to restore. No Trailing Slash.</param>
        /// <param name="outputDirectory">Local directory to save downloaded files</param>
        /// <param name="fileExtensions">Only download files ending with file extensions in this array of file extensions.</param>
        /// <param name="GetSubFolders"></param>
        /// <param name="excludeMailboxConfig"></param>
        /// <param name="assumeGZ">Assume the files are gzipped on AWS and add .gz to the end of the file name before saving it locally.</param>
        /// <param name="extractGZ">Extract gzipped files and delete the gzip file</param>
        public static void RetrieveFromS3(string AWSPath, string outputDirectory, string[] fileExtensions, bool GetSubFolders, bool excludeMailboxConfig, bool assumeGZ, bool extractGZ)
        {
            AWSS3Helper h = new AWSS3Helper("Your Access Key ID", "Your Secrety Key", "Your Bucket Name");

            List<string> files = h.GetFileList(AWSPath, GetSubFolders, true).ToList();

            if (fileExtensions != null) files = files.Where(p => fileExtensions.Contains(Path.GetExtension(p).ToLower())).ToList();
            if (excludeMailboxConfig) files = files.Where(p => Path.GetFileName(p).ToLower() != "mailbox.cfg").ToList();

            foreach (string file in files)
            {
                if (file.Right(1) != "/")
                {
                    Console.WriteLine(file);
                    string localFilePath = AppDomain.CurrentDomain.BaseDirectory + "/" + outputDirectory + file.Replace(AWSPath, "");
                    string gzipedFilePath = localFilePath + (assumeGZ?".gz":"");
                    try { 
                        Directory.CreateDirectory(Path.GetDirectoryName(localFilePath));
                        if (!File.Exists(localFilePath))
                        {
                            if (h.DownloadFile(file, gzipedFilePath, false, false))
                            {
                                if (extractGZ) ExtractGZip(gzipedFilePath, true);
                            }
                        }
                    }
                    catch {
                        //(Exception e) {
                        //Console.WriteLine("Error Downloading File! " + e.Message);
                    }
                }
            }
        }

        // Extracts the file contained within a GZip
        public static void ExtractGZip(string gzipFileName, bool DeleteOriginal = false)
        {
            // Use a 4K buffer. Any larger is a waste.    
            byte[] dataBuffer = new byte[4096];

            using (System.IO.Stream fs = new FileStream(gzipFileName, FileMode.Open, FileAccess.Read))
            {
                using (GZipInputStream gzipStream = new GZipInputStream(fs))
                {

                    // Change this to your needs
                    string fnOut = Path.Combine(Path.GetDirectoryName(gzipFileName), Path.GetFileNameWithoutExtension(gzipFileName));

                    using (FileStream fsOut = File.Create(fnOut))
                    {
                        StreamUtils.Copy(gzipStream, fsOut, dataBuffer);
                    }
                }
            }

            if (DeleteOriginal)
            {
                File.Delete(gzipFileName);
            }
        }
    }
}
