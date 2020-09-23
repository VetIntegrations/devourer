from datetime import datetime
from invoke import task

from .utils import run_and_parse


@task
def build_image(ctx):
    tag_name = 'gcr.io/vis-dev-286313/devourer:v0.0.{}'.format(
        datetime.now().strftime('%Y%m%d%H%M')
    )
    ctx.run('docker build -t devourer -f build/Dockerfile .')
    ctx.run('docker tag devourer {}'.format(tag_name))
    ctx.run('docker tag devourer gcr.io/vis-dev-286313/devourer:latest')
    ctx.run('docker push {}'.format(tag_name))
    ctx.run('docker push gcr.io/vis-dev-286313/devourer:latest')
    print(tag_name)


@task
def deploy(ctx, project_id, image, envfile):
    """
    Deploy Docker image to GCP Cloud Engine
    """
    vm_list = run_and_parse(
        ctx,
        f'gcloud --project={project_id} compute instances list '
        '--filter "name=( \'vis-devourer\' )" '
        '--format json'
    )
    cmd = (
        f'gcloud --project={project_id} compute instances '
        '{action} vis-devourer '
        '--zone us-central1-a '
        f'--container-image {image} '
        f'--container-env-file {envfile} '
    )
    if len(vm_list) == 0:
        cmd = cmd.format(action='create-with-container')
        cmd += '--tags https-server,http-server '
    else:
        cmd = cmd.format(action='update-container')

    ctx.run(cmd)

    firewall_rules = run_and_parse(
        ctx,
        f'gcloud --project={project_id} compute firewall-rules list '
        '--filter "name=default-allow-http" --format json '
    )

    if len(firewall_rules) == 0:
        ctx.run(
            f'gcloud --project={project_id} compute firewall-rules create default-allow-https '
            '--direction=INGRESS '
            '--priority=1000 '
            '--network=default '
            '--action=ALLOW '
            '--rules=tcp:443 '
            '--source-ranges=0.0.0.0/0 '
            '--target-tags=https-server'
        )
        ctx.run(
            f'gcloud --project={project_id} compute firewall-rules create default-allow-http '
            '--direction=INGRESS '
            '--priority=1000 '
            '--network=default '
            '--action=ALLOW '
            '--rules=tcp:80 '
            '--source-ranges=0.0.0.0/0 '
            '--target-tags=http-server'
        )
