"""
module containing AWS SQS fns

flexible data passing between lambda fns

version 0.2.1
- all fns accept client

version 0.2.0
- added base64 decoder helper function

version 0.1.0:
- made code synchronous
- modified the message to pass a file name plus a data dict
"""


from botocore.exceptions import ClientError
import base64 as b64


version = '0.2.1'
date = '01 August 2018'
author = 'A Spence'


def decode_b64(base64_obj):
    """
    when sending binary data on SQS, aws converts binary and returns a base64 object this decodes it back to binary
    :param base64_obj:
    :return: binary 
    """
    return b64.b64decode(base64_obj)


def get_queue(sqs, logger, q_name: str):
    """
    get an AWS SQS  url synchronously, async version below

    Throws a ClientError exception if the HTTP Status is not 200 or there is not a Key:Value pair containing the Q url

    :param sqs: aws sqs client
    :param q_name: str: the name of the sqs Q
    :return: the q url
    """
    logger.debug('in get SQS url')

    try:
        response = sqs.get_queue_url(QueueName=q_name)
    except Exception as exp:
        logger.error('get_queue_url failed due to: {}'.format(exp))
        raise ClientError('no sqs url')
    else:
        logger.debug('queue url get response: {}'.format(response))

        # check http status
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ClientError('get sqs url: HTTPStatus != 200')

        # then check message sent success or fail
        try:
            response['QueueUrl']
        except KeyError:
            raise ClientError('no sqs queue url returned {}'.format(response))
        else:
            url = response['QueueUrl']
            logger.info('sqs url: {}'.format(response['QueueUrl']))

    return url


def send_message(sqs, logger, url, message):
    """
    put a message on AWS SQS synchronously

    check hhtp status and message send success in the response. If not 200 and Successful raise and exception and
    do a simply retry 3 times

    uses the boto3 logic for multiple messages but only structured to send one at a time. This api was cleaner so used
    it rather than single message

    :param sqs: sqs client
    :param url: str: sqs Q url
    :param message: list: sqs style formatted
    :return: the response
    """
    logger.debug('in send_message - to SQS url')

    try:
        response = sqs.send_message_batch(
            QueueUrl=url,
            Entries=message)
    except Exception as exp:
        logger.error("sqs send message failed: {}".format(exp))
        raise ClientError('sqs send message failure')
    else:
        logger.debug('send message response: {}'.format(response))

        # check http status
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ClientError('send message to sqs: HTTPStatus != 200')

        # then check message sent success or fail
        try:
            response['Successful']
        except KeyError:
            raise ClientError('message send failure to sqs {}'.format(message))

    return response


def get_msg(sqs, logger, url):
    """
    retrieve a message from aws SQS

    return control in event of Exceptions to build resilience

    :param sqs: sqs client
    :param url: sqs queue url
    :return: the response
    """

    logger.debug('in process_msg - get message from SQS')

    try:
        response = sqs.receive_message(QueueUrl=url, MessageAttributeNames=['All'])
    except ClientError:
        logger.error('sqs message receive - ClientError - url: {}'.format(url))
        raise ClientError
    else:
        logger.debug('sqs: {} - message RECEIVE response: {}'.format(url, response))

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error('sqs: {} -  message received HTTPStatus != 200'.format(url))
            raise ClientError

    return response


def del_message(sqs, logger, url, handle):
    """
    delete an aws SQS message

    :param client: sqs client
    :param url: str: sqs url
    :param handle: message receipt handle for the message to be deleted
    :return: the response
    """
    logger.debug('in del_message - delete SQS message')

    try:
        del_response = sqs.delete_message(
            QueueUrl=url,
            ReceiptHandle=handle
        )
    except ClientError:
        raise ClientError
    else:
        logger.debug('sqs: {} - msg del response: {} - msg: {}'.format(url, del_response, handle))
        if del_response['ResponseMetadata']['HTTPStatusCode'] != 200:
            logger.error('HTTPStatus error -retrying')
            raise ClientError('sqs: {} - message delete HTTPStatus != 200 - msg: {}'.format(url, handle))
        else:
            logger.debug('sqs: {} - MSG DELETED SUCCESSFULLY {} '.format(url, handle))

    return


async def purge_sqs(client, logger, url):
    """only allowed to do this once ever 60 seconds or may raise and exception"""

    logger.debug('>>>> in async purge_sqs - purge the whole SQS')

    try:
        purge_resp = await client.purge_queue(QueueUrl=url)
    except ClientError as exp:
        logger.error(exp)
        purge_resp = ' '
    else:
        if purge_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise ClientError('sqs purge HTTPStatus != 200')
        logger.debug('q purge response: {}'.format(purge_resp))

    return purge_resp
