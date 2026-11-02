import json

import boto3


class PeerModel:
    """Lambda, ECS, EC2 network lookup, and SSM reads for a peer."""

    def invoke(self, function_name, payload, invocation_type, region):
        client = boto3.client("lambda", region_name=region)
        body = payload if isinstance(payload, str) else json.dumps(payload)
        return client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            Payload=body,
        )

    def function_configuration(self, function_name, region):
        client = boto3.client("lambda", region_name=region)
        return client.get_function_configuration(FunctionName=function_name)

    def update_environment(self, function_name, region, variables):
        client = boto3.client("lambda", region_name=region)
        return client.update_function_configuration(
            FunctionName=function_name,
            Environment={"Variables": variables},
        )

    def run_task(self, region, **params):
        client = boto3.client("ecs", region_name=region)
        return client.run_task(**params)

    def default_network(self, region):
        result = {"subnets": [], "security_groups": []}
        ec2 = boto3.client("ec2", region_name=region)
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}]).get("Vpcs", [])
        if not vpcs:
            return result
        vpc_id = vpcs[0]["VpcId"]
        subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]).get("Subnets", [])
        result["subnets"] = [item["SubnetId"] for item in subnets if item.get("SubnetId")]
        groups = ec2.describe_security_groups(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": ["default"]},
            ]
        ).get("SecurityGroups", [])
        if groups and groups[0].get("GroupId"):
            result["security_groups"] = [groups[0]["GroupId"]]
        return result

    def parameter_json(self, path, region):
        raw = boto3.client("ssm", region_name=region).get_parameter(Name=path, WithDecryption=True)
        value = str(raw["Parameter"]["Value"]).strip()
        if not value:
            return None
        return json.loads(value)
