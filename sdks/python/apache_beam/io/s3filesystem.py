from __future__ import absolute_import

import boto3
import botocore
import datetime
import io
import logging
import pytz
import re

from apache_beam.io import filesystemio
from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem
from apache_beam.options.pipeline_options import AwsOptions
from apache_beam.options.pipeline_options import PipelineOptions

__all__ = ['S3FileSystem']

DEFAULT_READ_BUFFER_SIZE = 16 * 1024 * 1024

class S3Downloader(filesystemio.Downloader):

  def __init__(self, s3_client, bucket, object_path):
    self._s3_client = s3_client
    self._bucket = bucket
    self._object_path = object_path
    self._size = self._s3_client.head_object(Bucket=bucket, Key=self._object_path)["ContentLength"]

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    response = self._s3_client.get_object(
        Bucket=self._bucket, 
        Key=self._object_path, 
        Range="bytes={}-{}".format(str(start), str(end)))
    return response["Body"].read()

class S3Uploader(filesystemio.Uploader):
    #TODO: Implement S3Uploader

  def __init__(self, s3_client, bucket, object_path):
    self._s3_client = s3_client
    self._bucket = bucket
    self._object_path = object_path

  def put(self, data):
    pass

  def finish(self):
    pass

class S3FileSystem(FileSystem):

    """``FileSystem`` implementation that supports S3

    URL arguments to methods expect strings starting with ``s3://``.
    """
    def __init__(self, pipeline_options):
        """Initializes a connection to S3 through boto3.

        Connection configuration is done by passing pipeline options.
        See :class:`~apache_beam.options.pipeline_options.AwsOptions`.
        """
        super(S3FileSystem, self).__init__(pipeline_options)
        logging.getLogger("boto3.client").setLevel(logging.WARN)
        if pipeline_options is None:
          raise ValueError("pipeline_options is not set")
        if isinstance(pipeline_options, PipelineOptions):
          aws_options = pipeline_options.view_as(AwsOptions)
          aws_access_key = aws_options.aws_access_key
          aws_secret_key = aws_options.aws_secret_key
        else:
          aws_access_key = pipeline_options.get("aws_access_key")
          aws_secret_key = pipeline_options.get("aws_secret_key")
        if aws_access_key is None:
          raise ValueError("aws_access_key is not set")
        if aws_secret_key is None:
          raise ValueError("aws_secret_key is not set")

        self._s3_client = boto3.client(
            "s3", 
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key)

    @classmethod
    def scheme(cls):
        return "s3"

    @classmethod
    def prefix(cls):
        return "%s://" % cls.scheme()

    @staticmethod
    def parse_path(s3_path):
        """ 
        Parses an s3 url and extracts the bucket and object_path
        i.e
         s3_url = "s3://my_bucket/path/to"
         bucket, object_path = S3FileSystem(s3_url)
         bucket == "my_bucket"
         path == "path/to"
        """
        match = re.match("^s3://([^/]+)/(.*)$", s3_path)
        if match is None or match.group(2) == "":
            raise ValueError("S3 url must be in the form s3://<bucket>/<object>.")
        return match.group(1), match.group(2)

    def get_object_metadata(self, bucket, key):
        """
        Helper function to retrieve object metadata
        
        Returns:
            [type] -- [description]
        """
        try:
            return self._s3_client.head_object(Bucket=bucket, Key=key)
        except botocore.exceptions.ClientError:
            logging.warning("No object metadata found for for %s%s/%s" %(self.prefix(), bucket, key))
            return None

    def open(self, path, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
        """Returns a read channel for the given file path.

        Args:
          path: string path of the file object to be read
          mime_type: MIME type to specify the type of content in the file object
          compression_type: Type of compression to be used for this object

        Returns: file handle with a close function for the user to use
        """
        bucket, object_path = self.parse_path(path)
        stream = io.BufferedReader(
        filesystemio.DownloaderStream(
            S3Downloader(self._s3_client, bucket, object_path)),
        buffer_size=DEFAULT_READ_BUFFER_SIZE)
        return self._add_compression(stream, path, mime_type, compression_type)

    @staticmethod
    def _add_compression(stream, path, mime_type, compression_type):
        """
        Helper function to open file-like-objects
        based on  given compression mode or file extension

         Args:
          stream : file-like object
          path: string path of the file object to be read
          mime_type: MIME type to specify the type of content in the file object
          compression_type: Type of compression to be used for this object
        
        Returns:
            File-like object
        """
        if mime_type != "application/octet-stream":
          logging.warning("Mime types are not supported. Got non-default mime_type:"
                          "%s", mime_type)
        if compression_type == CompressionTypes.AUTO:
          compression_type = CompressionTypes.detect_compression_type(path)
        if compression_type != CompressionTypes.UNCOMPRESSED:
          return CompressedFile(stream)
        return stream

    def join(self, basepath, *paths):
        if not basepath.startswith(self.prefix()):
          raise ValueError("Basepath %r is not a valid S3 path" % basepath)
        path = basepath
        for p in paths:
          path = path.rstrip('/') + '/' + p.lstrip('/')
        return path

    
    def split(self, path):
        prefix = self.prefix()
        if not path.startswith(prefix):
            raise ValueError("Path %r does not begin with valid S3 prefix %s" % (path, prefix))
        prefix_length = len(prefix)
        for i in range(len(path)-1, prefix_length, -1): 
            if path[i] == "/":
                j = i + 1
                head = path[:i]
                tail = path[j:]
                return head,tail
        return path, ""

   
    def mkdirs(self, path):
        """ 
        S3 has not notions of directories , hence this method is not implemented.       
        Raises:
            NotImplementedError 
        """
        raise NotImplementedError

    
    def has_dirs(self):
        return False

    def _list_prefix(self, prefix):
        bucket, prefix_path = self.parse_path(prefix)
        kwargs = {'Bucket': bucket, 'Prefix': prefix_path}
        objects = {}
        while True:
            resp = self._s3_client.list_objects_v2(**kwargs)
            for obj in resp["Contents"]:
                key = obj["Key"]
                size = obj["Size"]
                path = "s3://%s/%s" % (bucket, key)
                if key.endswith("/"):
                    continue
                objects[path] =  size
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        return objects

    def _list(self, dir_or_prefix):
        objects = self._list_prefix(dir_or_prefix)
        for path in objects:
            yield FileMetadata(path, objects[path])

    def create(self, path, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
        """[summary]
        
        [description]
        
        Arguments:
            path {[type]} -- [description]
        
        Keyword Arguments:
            mime_type {str} -- [description] (default: {'application/octet-stream'})
            compression_type {[type]} -- [description] (default: {CompressionTypes.AUTO})
        """
        bucket, object_path = self.parse_path(path)
        stream = io.BufferedWriter(
        filesystemio.UploaderStream(
            S3Uploader(self._s3_client, bucket, object_path)),
        buffer_size=DEFAULT_READ_BUFFER_SIZE)
        return self._add_compression(stream, path, mime_type, compression_type)
        

    def copy(self, source_file_names, destination_file_names):
        if len(source_file_names) != len(destination_file_names):
          raise BeamIOError(
              'source_file_names and destination_file_names should '
              'be equal in length: %d != %d' % (
                  len(source_file_names), len(destination_file_names)))

        for i in range(len(source_file_names)):
            source_path = source_file_names[i]
            destination_path = destination_file_names[i]
            if source_path.endswith("/") and destination_path.endswith("/"):
                src_files = self._list_prefix(source_path)
                for src_file in src_files:
                    src_file_name = src_file.split("/")[-1]
                    destination_file = destination_path +  src_file_name
                    self._copy_object(src_file, destination_file)
            else:
                self._copy_object(source_path, destination_path)


    def _copy_object(self, source_file_name, destination_file_name):
        src_bucket, src_prefix_path = self.parse_path(source_file_name)
        dst_bucket, dst_prefix_path = self.parse_path(destination_file_name)

        copy_source = {"Bucket" : src_bucket, "Key" : src_prefix_path}
        try:
            self._s3_client.copy(Bucket=dst_bucket, Key=dst_prefix_path, CopySource=copy_source)
        except:
            raise BeamIOError("Unable to copy object s3://%s/%s to s3://%s/%s" % (src_bucket, src_prefix_path, dst_bucket, dst_prefix_path))

    
    def rename(self, source_file_names, destination_file_names):
        self.copy(source_file_names, destination_file_names)
        self.delete(source_file_names)

    def exists(self, path):
        exists = False
        bucket, object_path = self.parse_path(path)
        object_metadata = self.get_object_metadata(bucket, object_path)
        if object_metadata:
            exists = True
        return exists

    def size(self, path):
        bucket, object_path = self.parse_path(path)
        object_metadata = self.get_object_metadata(bucket, object_path)
        if object_metadata is None:
            raise BeamIOError('File not found: %s' % path)
        return object_metadata["ContentLength"]


    def last_updated(self, path):
        epoch_dttm = datetime.datetime(1970,1,1).replace(tzinfo=pytz.utc)
        bucket, object_path = self.parse_path(path)
        object_metadata = self.get_object_metadata(bucket, object_path)
        if object_metadata is None:
            raise BeamIOError('File not found: %s' % path)
        last_modified_dttm = object_metadata["LastModified"].replace(tzinfo=pytz.utc)
        return (last_modified_dttm - epoch_dttm).total_seconds()

    def delete(self, paths):
        """ Deletes files or directories at the provided paths.
            Directories will be deleted recursively.
            Args:
              paths: list of paths that give the file objects to be deleted
        """
        for path in paths:
            if path.endswith("/"):
                objects_to_be_deleted = self._list_prefix(path)
                for obj_path in objects_to_be_deleted:
                    bucket, object_path = self.parse_path(obj_path)
                    self._s3_client.delete_object(Bucket=bucket, Key=obj_path)
            else:
                bucket, object_path = self.parse_path(path)
                self._s3_client.delete_object(Bucket=bucket, Key=object_path)
            


    