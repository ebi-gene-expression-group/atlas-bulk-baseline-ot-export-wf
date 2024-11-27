# How to run

This has been added to run from Jenkins

# Upload error

It is possible that when you try to run this after some time the upload part fails, with an error that can be found in the upload_ot_baseline log:

```
cat upload_ot_baseline_export/00/upload_ot_baseline_export.log
```

if the error looks like this:

```
Your credentials are invalid. Please run
$ gcloud auth login
```

To run that you need to go inside the conda environment being used for that rule, which you can find by looking at the snakemake logs (see last line in the below code snippet):

```
Submitted job 5 with external jobid '833475 logs/cluster/upload_ot_baseline_export/chunk=00/jobid5_bb326614-43eb-4265-aba1-16493a91
cf43.out'.
[Mon Oct 10 22:32:05 2022]
Error in rule upload_ot_baseline_export:
    jobid: 5
    output: upload_ot_baseline_export/00/done
    log: upload_ot_baseline_export/00/upload_ot_baseline_export.log (check log file(s) for error message)
    conda-env: <path-to>/snakemake_conda_envs/b73382db313a98c7af5f1a819aba6048
```

1) If gcloud complains that it is too old when trying to do `gcloud auth login`.

In that case, update `envs/gsutil.yaml` to the latest available version of google-cloud-sdk.

2) If gcloud complains that credentials are invalid.

In that case Please activate environment as mentioned above then run `gcloud auth login` it will show a URL and ask for token. Go to URL and it will show a token, paste it on terminal and authenticate. Then re-run export job again.

Also, when trying to do gcloud auth login, it requires a user that has write access to that GCP bucket (for this, please liaise with OpenTargets so that your EBI Google at work account is added).
