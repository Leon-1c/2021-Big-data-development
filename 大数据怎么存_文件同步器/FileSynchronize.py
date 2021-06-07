import boto3
import os
import time
import hashlib
import math
from boto3.session import Session

service_name = 's3'
bucketName = 'tiit'
access_key = ''
secret_key = ''
endpoint_url = ''
src_dir = r'C:\tiit'   # 源文件目录地址
# Max size in bytes before uploading in parts.
AWS_UPLOAD_MAX_SIZE = 20 * 1024 * 1024
# Size of parts when uploading in parts
AWS_UPLOAD_PART_SIZE = 5 * 1024 * 1024


def md5sum(sourcePath):

    filesize = os.path.getsize(sourcePath)
    hash = hashlib.md5()

    if filesize > AWS_UPLOAD_MAX_SIZE:

        block_count = 0
        md5string = []
        with open(sourcePath, "rb") as f:
            for block in iter(lambda: f.read(AWS_UPLOAD_PART_SIZE), b""):
                # hash = hashlib.md5()
                md5string.append(hashlib.md5(block))
                block_count += 1

        digests = b''.join(m.digest() for m in md5string)
        digests_md5 = hashlib.md5(digests)
        # hash = hashlib.md5()
        return digests_md5.hexdigest() + "-" + str(block_count)

    else:
        with open(sourcePath, "rb") as f:
            for block in iter(lambda: f.read(AWS_UPLOAD_PART_SIZE), b""):
                hash.update(block)
        return hash.hexdigest()


def gci(nlist, filepath):
    # 遍历filepath下所有文件，包括子目录
    files = os.listdir(filepath)
    for fi in files:
        fi_d = os.path.join(filepath, fi)
        if os.path.isdir(fi_d):
            gci(nlist, fi_d)
        else:
            temp = os.path.join(filepath, fi_d)
            nlist.append(temp.replace('\\', '/').split(bucketName+'/')[1])


def connect():
    '''create the client connect with s3'''
    session = Session(access_key, secret_key)
    s3_client = session.client(service_name, endpoint_url=endpoint_url)
    return s3_client
    # return s3.Bucket(bucketName)


def get_bucket_filename(s3_client):
    '''get the filename in bucket , return a dict'''
    ndict = {}
    olist = s3_client.list_objects(Bucket="tiit")

    if 'Contents' not in olist:
        return ndict
    for o in olist['Contents']:
        key = o['Key']
        if(key[-1] != '/'):
            ETag = o['ETag']
            head = s3_client.head_object(Bucket=bucketName, Key=key)
            Time = head['Metadata']['Lastmodifytime']
            ndict[key] = [Time, eval(ETag)]

    # return all object name in bucket
    return ndict


def get_local_filename():
    '''get the local filename, return a dict'''
    ndict = {}
    nlist = []
    gci(nlist, src_dir)
    for i in nlist:
        # get the file lastmodifytime
        time = os.path.getmtime(src_dir+'\\'+i)
        # add {key : lastmodifytime} and md5 to ndict
        ndict[i] = [str(time), md5sum(src_dir+'\\'+i)]

    # return all file name in local folder
    return ndict


def upload(s3_client, filename):
    '''upload the local file to bucket'''
    temp = filename.rsplit('/', 1)[0]
    response = s3_client.list_multipart_uploads(
        Bucket=bucketName, Prefix=filename)
    if filename != temp:
        s3_client.put_object(Bucket=bucketName, Key=(temp+'/'))

    if os.path.getsize(src_dir+'/'+filename) < AWS_UPLOAD_MAX_SIZE:
        s3_client.put_object(Bucket=bucketName, Key=filename,
                             Body=open(src_dir+'/'+filename, 'rb').read())
    elif 'Uploads' in response:
        # recover mutipart upload
        upID = response[u'Uploads'][0][u'UploadId']
        resp = s3_client.list_parts(
            Bucket=bucketName, Key=filename, UploadId=upID)
        num = []
        for part in resp['Parts']:
            num.append(part['PartNumber'])

        part_info = {'Parts': []}
        cnt = 1
        file = open(src_dir+'\\'+filename, 'rb')
        while 1:
            data = file.read(AWS_UPLOAD_PART_SIZE)
            if data == b'':
                break
            elif cnt not in num:
                response = s3_client.upload_part(
                    Bucket=bucketName, Key=filename, PartNumber=cnt, UploadId=upID, Body=data)
                part_info['Parts'].append(
                    {'PartNumber': cnt, 'ETag': response['ETag']})
                print("Uploading %s's part %d" % (filename,cnt))
                cnt += 1
            else:
                part_info['Parts'].append(
                    {'PartNumber': cnt, 'ETag':  resp['Parts'][cnt-num[0]]['ETag']})
                cnt += 1
        # complete mutipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucketName, Key=filename, UploadId=upID, MultipartUpload=part_info)
    else:
        # mutipart upload
        mpu = s3_client.create_multipart_upload(
            Bucket=bucketName, Key=filename, StorageClass='STANDARD')
        part_info = {'Parts': []}
        cnt = 1
        file = open(src_dir+'\\'+filename, 'rb')
        while 1:
            data = file.read(AWS_UPLOAD_PART_SIZE)
            if data == b'':
                break
            response = s3_client.upload_part(
                Bucket=bucketName, Key=filename, PartNumber=cnt, UploadId=mpu["UploadId"], Body=data)
            part_info['Parts'].append(
                {'PartNumber': cnt, 'ETag': response['ETag']})
            print("Uploading file %s's part %d" % (filename,cnt))
            cnt += 1
        # complete mutipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucketName, Key=filename, UploadId=mpu["UploadId"], MultipartUpload=part_info)

    print('Upload file ', filename, ' succeed!')


def download(s3_client, key):
    '''download the bucket object'''
    if key.find('/') != -1:
        path = key.rsplit('/', 1)[0]
        if not os.path.isdir(src_dir+'/'+path):
            os.makedirs(src_dir+'/'+path)

    f = open(src_dir+'/'+key, 'ab')
    resp = s3_client.get_object(Bucket=bucketName, Key=key)

    if resp['ContentLength'] > AWS_UPLOAD_MAX_SIZE:
        begin = math.ceil(os.path.getsize(src_dir+'/'+key)/AWS_UPLOAD_PART_SIZE)

        for i in range(begin, math.ceil(resp['ContentLength']/AWS_UPLOAD_PART_SIZE)):
            offset = AWS_UPLOAD_PART_SIZE*i
            response = s3_client.get_object(
                Bucket=bucketName, Key=key, Range="bytes=%d-%d" % (offset, offset+AWS_UPLOAD_PART_SIZE))
            f.write(response['Body'].read(
                min(AWS_UPLOAD_PART_SIZE, resp['ContentLength']-offset)))
            print("Downloading file %s's part %d" % (key,i))

        f.close()
    else:
        f.write(resp['Body'].read())
        f.close()

    print('Download file ', key, ' succeed!')


def comp(local_list, bucket_list, key):
    if local_list[key][1] == bucket_list[key][1]:
        return 0
    else:
        if local_list[key][0] > bucket_list[key][0]:
            return 1
        else:
            return -1


def synchronize(s3_client, mod):
    '''synchronize the file between local and bucket'''
    local_list = get_local_filename()
    bucket_list = get_bucket_filename(s3_client)

    print('\nBegin to synchronize in %s mod......' % mod)
    # synchronize the file base local file
    if mod == 'local':
        for lkey in local_list.keys():
            if lkey in bucket_list:
                sign = comp(local_list, bucket_list, lkey)
                if sign > 0:
                    upload(s3_client, lkey)
                elif sign < 0:
                    download(s3_client, lkey)
            else:
                upload(s3_client, lkey)

        for bkey in bucket_list.keys():
            if bkey not in local_list:
                # delete the bucket file not in local
                s3_client.delete_object(Bucket=bucketName, Key=bkey)
                print('Delete bucket file %s succeed!' % bkey)
    # synchronize the file base bucket file
    elif mod == 'bucket':
        for bkey in bucket_list.keys():
            if bkey in local_list:
                sign = comp(local_list, bucket_list, bkey)
                if sign != 0:
                    download(s3_client, bkey)
            else:
                download(s3_client, bkey)

        for lkey in local_list.keys():
            if lkey not in bucket_list:
                # delete the local file not in bucket
                os.remove(src_dir+'/'+lkey)
                print('Delete local file %s succeed!' % lkey)

    print('Synchronize in %s mod succeed!' % mod)


def main():
    s3_client = connect()
    while(True):
        mod = input(
            '\nPlease enter local/bucket/q/c to chose the synchronize mode: ')
        # mod = 'local'
        if mod == 'q':
            break
        elif mod == 'local' or mod == 'bucket':
            synchronize(s3_client, mod)
        elif mod == 'c':
            print('Output will be clear in 5 seconds......')
            time.sleep(5)
            os.system('cls')
        else:
            print('Please enter the correct synchronize mod!\n')


if __name__ == "__main__":
    main()