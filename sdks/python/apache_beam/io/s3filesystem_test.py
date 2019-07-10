import boto3
from moto import mock_s3
import logging
import unittest

from apache_beam.io.s3filesystem import S3FileSystem
from apache_beam.io.filesystem import BeamIOError
from apache_beam.options.pipeline_options import PipelineOptions

class S3MockObject(object):
    def __init__(self, object_path, value):
        self.object_path = object_path
        self.value = value

    def save(self, bucket_name):
        s3 = boto3.client("s3")
        s3.put_object(Bucket=bucket_name, Key=self.object_path, Body=self.value)

class S3FileSystemTest(unittest.TestCase):

    def setUp(self):
        pipeline_options = {
            "aws_access_key" : "",
            "aws_secret_key" : ""
        }
        self.fs = S3FileSystem(pipeline_options)
        self.tmp_bucket = "tmp_bucket"
        self.s3_client = boto3.client("s3")

    def test_scheme(self):
        expected= "s3"
        self.assertEqual(self.fs.scheme(), expected)
        self.assertEqual(S3FileSystem.scheme(), expected)

    @mock_s3
    def test_open(self):
        object_content = "The expected file contents"
        object_path = "mypath/file.json"
        self.s3_client.create_bucket(Bucket=self.tmp_bucket)

        mock_object = S3MockObject(object_path, object_content)
        mock_object.save(self.tmp_bucket)

        actual_content = self.fs.open("s3://%s/%s" %(self.tmp_bucket, object_path))
        assert actual_content.read() == object_content

    def test_join(self):
        self.assertEqual('s3://bucket/path/to/file',
                         self.fs.join('s3://bucket/path', 'to', 'file'))
        self.assertEqual('s3://bucket/path/to/file',
                         self.fs.join('s3://bucket/path', 'to/file'))
        self.assertEqual('s3://bucket/path/to/file',
                         self.fs.join('s3://bucket/path', '/to/file'))
        self.assertEqual('s3://bucket/path/to/file',
                         self.fs.join('s3://bucket/path/', 'to', 'file'))
        self.assertEqual('s3://bucket/path/to/file',
                         self.fs.join('s3://bucket/path/', 'to/file'))
        self.assertEqual('s3://bucket/path/to/file',
                         self.fs.join('s3://bucket/path/', '/to/file'))
        with self.assertRaises(ValueError):
          self.fs.join('/bucket/path/', '/to/file')

    def test_split(self):
        self.assertEqual(("s3://foo/bar", "baz"),
                     self.fs.split("s3://foo/bar/baz"))
        self.assertEqual(("s3://foo", ""),
                     self.fs.split("s3://foo/"))
        self.assertEqual(("s3://foo", ""),
                         self.fs.split("s3://foo"))

        with self.assertRaises(ValueError):
          self.fs.split("/no/s3/prefix")

    @mock_s3
    def test_copy(self):
        object_content = '{"key" : "val"}'
        object_path = "mypath/file.json"
        expected_copy_object_path = "copied/to/another/locaiton/file_copy.json"
        self.s3_client.create_bucket(Bucket=self.tmp_bucket)

        mock_object = S3MockObject(object_path, object_content)
        mock_object.save(self.tmp_bucket)

        copy_src = "s3://%s/%s" % (self.tmp_bucket, object_path)
        copy_dst = "s3://%s/%s" % (self.tmp_bucket, expected_copy_object_path)

        self.fs.copy([copy_src], [copy_dst])

        assert self.fs.exists(copy_dst) == True

    @mock_s3
    def test_copy_directories(self):
        object_content = '{"key" : "val"}'
        src_prefix = "path/to/src/folder/"
        dst_prefix = "some/other/path/to/test/files/"
        src_files = ["file1.json", "file2.json", "file3.json"]

        self.s3_client.create_bucket(Bucket=self.tmp_bucket)

        for src_file in src_files:
            object_path = src_prefix + src_file
            mock_object = S3MockObject(object_path, object_content)
            mock_object.save(self.tmp_bucket)

        copy_src = "s3://%s/%s" % (self.tmp_bucket, src_prefix)
        copy_dst = "s3://%s/%s" % (self.tmp_bucket, dst_prefix)

        self.fs.copy([copy_src], [copy_dst])

        for file_name in src_files:
            copied_object =  "s3://%s/%s%s" % (self.tmp_bucket, dst_prefix, file_name)
            assert self.fs.exists(copied_object) == True

    @mock_s3
    def test_rename(self):
        object_content = '{"key" : "val"}'
        object_path = "mypath/file.json"
        expected_copy_object_path = "copied/to/another/locaiton/file_copy.json"
        self.s3_client.create_bucket(Bucket=self.tmp_bucket)

        mock_object = S3MockObject(object_path, object_content)
        mock_object.save(self.tmp_bucket)

        copy_src = "s3://%s/%s" % (self.tmp_bucket, object_path)
        copy_dst = "s3://%s/%s" % (self.tmp_bucket, expected_copy_object_path)

        self.fs.rename([copy_src], [copy_dst])

        for i in range(len(copy_src)):
            src_object = copy_src
            dst_object = copy_dst
            assert self.fs.exists(src_object) == False and \
                self.fs.exists(dst_object) == True


    @mock_s3
    def test_exists(self):
        object_content = "Some file content"
        object_path = "mypath/file.json"
        self.s3_client.create_bucket(Bucket=self.tmp_bucket)

        mock_object = S3MockObject(object_path, object_content)
        mock_object.save(self.tmp_bucket)

        assert self.fs.exists(
            "s3://%s/%s" %(self.tmp_bucket, object_path)) == True

        assert self.fs.exists(
            "s3://non_existent/object") == False

    @mock_s3
    def test_size(self):
        object_content = '{"key" : "val"}'
        object_path = "mypath/file.json"
        self.s3_client.create_bucket(Bucket=self.tmp_bucket)

        mock_object = S3MockObject(object_path, object_content)
        mock_object.save(self.tmp_bucket)

        assert self.fs.size(
            "s3://%s/%s" %(self.tmp_bucket, object_path)) == len(object_content)

        with self.assertRaises(BeamIOError):
            self.fs.size(
                "s3://non_existent/object")

    @mock_s3
    def test_delete(self):
        object_content = '{"key" : "val"}'
        object_path = "mypath/file.json"

        self.s3_client.create_bucket(Bucket=self.tmp_bucket)

        mock_object = S3MockObject(object_path, object_content)
        mock_object.save(self.tmp_bucket)

        objects_to_be_deleted = [
            "s3://%s/%s" %(self.tmp_bucket, object_path)
        ]

        self.fs.delete(objects_to_be_deleted)

        for path in objects_to_be_deleted:
            assert self.fs.exists(path) == False

    @mock_s3
    def test_delete_prefix(self):
        object_content = '{"key" : "val"}'
        object_prefix = "mypath/"
        files = ["file1.json", "file2.json", "file3.json"]

        self.s3_client.create_bucket(Bucket=self.tmp_bucket)

        for file_name in files:
            object_path =  object_prefix + file_name
            mock_object = S3MockObject(object_path, object_content)
            mock_object.save(self.tmp_bucket)


        objects_to_be_deleted = [
            "s3://%s/%s" %(self.tmp_bucket, object_prefix)
        ]

        self.fs.delete(objects_to_be_deleted)

        for path in objects_to_be_deleted:
            assert self.fs.exists(path) == False


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()


