from raven import Client

sentry_client = Client()


def capture_exception():
    sentry_client.captureException()
