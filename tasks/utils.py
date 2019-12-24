import json


class CommandError(Exception):
    ...


def run_and_parse(ctx, command):
    resp = ctx.run(command)
    if not resp.ok:
        raise CommandError(resp.stderr)

    return json.loads(resp.stdout)
