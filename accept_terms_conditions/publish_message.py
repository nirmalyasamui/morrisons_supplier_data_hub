from google.cloud import pubsub
import email_constants as constant
import os


def publish_message(attributes):
    try:
        project_id = os.environ.get(constant.PROJECT)
        topic_id = constant.TOPIC_ID

        publisher = pubsub.PublisherClient()
        # The `topic_path` method creates a fully qualified identifier
        # in the form `projects/{project_id}/topics/{topic_id}`
        topic_path = publisher.topic_path(project_id, topic_id)

        data_str = "Send message"
        # Data must be a bytestring
        data = data_str.encode("utf-8")
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path,
                                   data,
                                   forename=str(attributes[constant.EMAIL_ATTRIBUTE_RECIPIENT_FORENAME]) if constant.EMAIL_ATTRIBUTE_RECIPIENT_FORENAME in attributes else constant.EMAIL_ATTRIBUTE_RECIPIENT_FORENAME, 
                                   surname=str(attributes[constant.EMAIL_ATTRIBUTE_RECIPIENT_SURNAME]) if constant.EMAIL_ATTRIBUTE_RECIPIENT_SURNAME in attributes else constant.EMAIL_ATTRIBUTE_RECIPIENT_SURNAME, 
                                   refid=str(attributes[constant.EMAIL_ATTRIBUTE_RECIPIENT_REFID]) if constant.EMAIL_ATTRIBUTE_RECIPIENT_REFID in attributes else constant.EMAIL_ATTRIBUTE_RECIPIENT_REFID,
                                   action=str(attributes[constant.EMAIL_ATTRIBUTE_ACTION]) if constant.EMAIL_ATTRIBUTE_ACTION in attributes else constant.EMAIL_ATTRIBUTE_ACTION,
                                   remarks=str(attributes[constant.EMAIL_ATTRIBUTE_RECIPIENT_REMARKS]) if constant.EMAIL_ATTRIBUTE_RECIPIENT_REMARKS in attributes else constant.EMAIL_ATTRIBUTE_RECIPIENT_REMARKS,
                                   sender_mail=str(attributes[constant.EMAIL_ATTRIBUTE_TO_EMAIL_ID]) if constant.EMAIL_ATTRIBUTE_TO_EMAIL_ID in attributes else constant.EMAIL_ATTRIBUTE_TO_EMAIL_ID,
                                   notifyto=str(attributes[constant.EMAIL_ATTRIBUTE_NOTIFY_USER]) if constant.EMAIL_ATTRIBUTE_NOTIFY_USER in attributes else constant.EMAIL_ATTRIBUTE_NOTIFY_USER,
                                   company = str(attributes[constant.EMAIL_ATTRIBUTE_COMPANY]) if constant.EMAIL_ATTRIBUTE_COMPANY in attributes else constant.EMAIL_ATTRIBUTE_COMPANY,
                                   workemail = str(attributes[constant.EMAIL_ATTRIBUTE_WORK_EMAIL_ID]) if constant.EMAIL_ATTRIBUTE_WORK_EMAIL_ID in attributes else constant.EMAIL_ATTRIBUTE_WORK_EMAIL_ID,
                                   jobrole = str(attributes[constant.EMAIL_ATTRIBUTE_JOB_ROLE]) if constant.EMAIL_ATTRIBUTE_JOB_ROLE in attributes else constant.EMAIL_ATTRIBUTE_JOB_ROLE,
                                   usertype = str(attributes[constant.EMAIL_ATTRIBUTE_USER_TYPE]) if constant.EMAIL_ATTRIBUTE_USER_TYPE in attributes else constant.EMAIL_ATTRIBUTE_USER_TYPE,
                                   grantaccesslink = str(attributes[constant.EMAIL_ATTRIBUTE_GRANT_ACCESS_LINK]) if constant.EMAIL_ATTRIBUTE_GRANT_ACCESS_LINK in attributes else constant.EMAIL_ATTRIBUTE_GRANT_ACCESS_LINK,
                                   inactivedays = str(attributes[constant.EMAIL_ATTRIBUTE_INACTIVE_DAYS]) if constant.EMAIL_ATTRIBUTE_INACTIVE_DAYS in attributes else constant.EMAIL_ATTRIBUTE_INACTIVE_DAYS)

        print('future - ', future)
        print(f"Published messages to {topic_path}.")
        return True
    except Exception as e:
        print('exception in publish message - {}'.format(e))
        return False