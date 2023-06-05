from pydantic import BaseSettings


class ModuleSettings(BaseSettings):
    # Pydantic will read the environment variables in a case-insensitive way, to set values for below

    #All enviroment variables
    KAFKA_HOST: str
    KAFKA_PORT: str
    KAFKA_TOPIC: str

    MARIADB_HOST: str
    MARIADB_PORT: int
    MARIADB_USER: str
    MARIADB_ROOT_PASS: str
    MARIADB_PASS: str
    MARIADB_DBNAME: str

    S3_ENDPOINT_URL: str
    S3_ACCESS_KEY_ID: str
    S3_SECRET_ACCESS_KEY: str
    S3_REGION_NAME: str
    S3_BUCKET: str

    # class Config:
    #     env_file = "dev.env"    


Setting = ModuleSettings()
print(Setting)

