from aws_env import bootstrap_ssm_env

bootstrap_ssm_env()

from mangum import Mangum
from main import app

handler = Mangum(app, lifespan="off")
