import boto3
import json


def get_secret_data(secret_id):
    """
    This function is used to retrieve secret password.
    """
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=secret_id)
    data = response["SecretString"]
    return json.loads(data)


def get_db_password(rds_host):
    """
    Get the database password from RDS.
    """
    secretid_key_dict = {
        "dev": {
            "secret_id": "kloo-dev-environment-variables",
            "key": "Dev_DB_PASSWORD",
        },
        "stage": {
            "secret_id": "kloo-Stage-Environment-Variables",
            "key": "Stage_db_password",
        },
        "demo": {
            "secret_id": "kloo_environment_variables_demo",
            "key": "Demo_DB_Password",
        },
        "prod": {
            "secret_id": "kloo_environment_variable_prod",
            "key": "Production_DB_Password",
        },
    }

    environments_list = list(secretid_key_dict.keys())
    environment = next(
        (substring for substring in environments_list if substring in rds_host), None
    )

    if environment:
        secretid_key = secretid_key_dict[environment]
        secret_id = secretid_key["secret_id"]
        data2 = get_secret_data(secret_id)
        KLOO_DB_PASSWORD_KLOOCHATGPT = data2[secretid_key["key"]]

    else:
        KLOO_DB_PASSWORD_KLOOCHATGPT = None

    return KLOO_DB_PASSWORD_KLOOCHATGPT
