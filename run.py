from app.main import app
import settings

KOLEJKA_CONF_CONTENT = """
[default]
config = kolejka

[kolejka]
server = %s
username = %s
password = %s
"""


def create_kolejka_conf(overwrite: bool = False):
    if not settings.KOLEJKA_CONF.exists() or overwrite:
        with open(settings.KOLEJKA_CONF, 'wt') as f:
            f.write(KOLEJKA_CONF_CONTENT % (
                settings.KOLEJKA_SERVER,
                settings.KOLEJKA_USERNAME,
                settings.KOLEJKA_PASSWORD
            ))


if __name__ == "__main__":
    import uvicorn
    create_kolejka_conf()
    uvicorn.run(app, host=settings.SERVER_HOST, port=settings.SERVER_PORT)
