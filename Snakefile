

# atom: set grammar=python:

def species_for_db(species):
    """
    Atlas database and web app use
    species as "Homo sapiens" instead of "homo_sapiens".
    """
    return species.replace("_", " ").capitalize()

def aggregate_accessions_ot_baseline_uploads(wildcards):
    checkpoint_output = checkpoints.divide_accessions_into_chunks.get(**wildcards).output[0]
    return expand("upload_ot_baseline_export/{chunk}/done",
        chunk=glob_wildcards("baseline_accessions_{chunk}").chunk)

def aggregate_accessions_ot_baseline_exports(wildcards):
    checkpoint_output = checkpoints.divide_accessions_into_chunks.get(**wildcards).output[0]
    return expand("ot_baseline_export/{chunk}/done",
        chunk=glob_wildcards("baseline_accessions_{chunk}").chunk)

def get_ot_baseline_metadata_files(baseline_accessions, path):
    accessions = []
    with open(baseline_accessions, 'r') as f:
        accessions = [line.rstrip() for line in f]
    return expand(path+"/{accession}/{accession}.metadata.json", accession=accessions)


rule get_public_accessions_for_species:
    log: "get_public_accessions_for_species.log"
    params:
        species=species_for_db(config['species']),
        atlas_env_file=config['atlas_env_file']
    output:
        accessions=temp("species_accessions.txt"),
        baseline_accessions=temp("species_baseline_accessions.txt")
    conda: "envs/postgres.yaml"
    shell:
        """
        set -e # snakemake on the cluster doesn't stop on error when --keep-going is set
        exec &> "{log}"
        source {params.atlas_env_file}
        # for dbConnection

        psql -c "COPY (
                    SELECT accession FROM experiment
                    WHERE species = '{params.species}'
                    AND not private
                    ORDER BY last_update DESC) TO STDOUT WITH NULL AS ''" \
             -v ON_ERROR_STOP=1 $dbConnection > {output.accessions}
        psql -c "COPY (
                    SELECT accession FROM experiment
                    WHERE species = '{params.species}'
                    AND not private
                    AND type = 'RNASEQ_MRNA_BASELINE'
                    ORDER BY last_update DESC) TO STDOUT WITH NULL AS ''" \
             -v ON_ERROR_STOP=1 $dbConnection > {output.baseline_accessions}
        """

checkpoint divide_accessions_into_chunks:
    log: "divide_accessions_into_chunks.log"
    params:
        lines_per_split=50,
        lines_per_split_baseline=5
    input:
        accessions="species_accessions.txt",
        baseline_accessions="species_baseline_accessions.txt"
    output:
        done=touch("divide_accessions_into_chunks.done")
    shell:
        """
        # This will generate accessions_01, accessions_02, etc and the same for baseline_accessions_
        split -l {params.lines_per_split} -d {input.accessions} accessions_
        split -l {params.lines_per_split_baseline} -d {input.baseline_accessions} baseline_accessions_
        """

rule ot_baseline_export:
    log: "ot_baseline_export/{chunk}/ot_baseline_export.log"
    conda: "envs/ot_baseline_dump.yaml"
    input:
        accessions="baseline_accessions_{chunk}"
    params:
        atlas_env_file=config['atlas_env_file'],
        n_extra_path=config['n_extra_path'],
        prefix="ot_baseline_export/{chunk}"
    output:
        #metadata_files=get_ot_baseline_metadata_files("baseline_accessions_{chunk}", "ot_baseline_export/{chunk}"),
        done=touch("ot_baseline_export/{chunk}/done")
    shell:
        """
        set -e # snakemake on the cluster doesn't stop on error when --keep-going is set
        exec &> "{log}"

        source {params.atlas_env_file}
        # for ATLAS_PROD and ATLAS_EXPS

        for ACCESSION in $(cat {input.accessions}); do
            DONE_FILE={params.prefix}/$ACCESSION.done
            if [ ! -f $DONE_FILE ]; then
                echo "Running $ACCESSION"
                mkdir -p {params.prefix}/$ACCESSION
                python {workflow.basedir}/scripts/ot-export-atlas-baseline-exp.py \
                    -p $ATLAS_EXPS/$ACCESSION -o {params.prefix}/$ACCESSION -n $ATLAS_PROD/{params.n_extra_path}/$ACCESSION
                touch $DONE_FILE
            else
                echo "$ACCESSION already done"
            fi
        done

        """


rule upload_ot_baseline_export:
    log: "upload_ot_baseline_export/{chunk}/upload_ot_baseline_export.log"
    conda: "envs/gsutil.yaml"
    params:
        atlas_bucket="atlas_baseline_expression",
        atlas_release=config['atlas_release'],
        timestamp=config['timestamp'],
        dump_prefix="ot_baseline_export/{chunk}",
        prefix="upload_ot_baseline_export/{chunk}"
    input:
        accessions="baseline_accessions_{chunk}",
        ot_baseline_export_done=rules.ot_baseline_export.output.done
    output:
        done=touch("upload_ot_baseline_export/{chunk}/done")
    shell:
        """
        set -e # snakemake on the cluster doesn't stop on error when --keep-going is set
        mkdir -p upload_ot_baseline_export/{wildcards.chunk}
        exec &> "{log}"


        GS_PREFIX="gs://{params.atlas_bucket}/rel_{params.atlas_release}_{params.timestamp}"
        DUMP_PREFIX={params.dump_prefix}
        UPLOAD_PREFIX={params.prefix}

        for ACCESSION in $(cat {input.accessions}); do

            UPLOADED_M_FILE=$UPLOAD_PREFIX/${{ACCESSION}}_metadata.uploaded
            if [ ! -f $UPLOADED_M_FILE ]; then
                gsutil cp $DUMP_PREFIX/$ACCESSION/${{ACCESSION}}.metadata.json \
                    $GS_PREFIX/metadata/$ACCESSION/${{ACCESSION}}.metadata.json
                touch $UPLOADED_M_FILE
            fi

            for metric in tpms fpkms; do
                UPLOADED_FILE=$UPLOAD_PREFIX/${{ACCESSION}}_aggregated_data_${{metric}}.uploaded
                JSONL=$DUMP_PREFIX/$ACCESSION/${{ACCESSION}}-expression-data-${{metric}}.jsonl.bz2
                echo "Checking $JSONL to upload..."
                if [ -f $JSONL ] && [ ! -f $UPLOADED_FILE ]; then
                    echo "...uploading"
                    gsutil cp $JSONL $GS_PREFIX/aggregated_data/$ACCESSION/${{ACCESSION}}-expression-data-${{metric}}.jsonl.bz2
                    touch $UPLOADED_FILE
                fi

                UPLOADED_FILE=$UPLOAD_PREFIX/${{ACCESSION}}_unaggregated_data_${{metric}}.uploaded
                JSONL=$DUMP_PREFIX/$ACCESSION/${{ACCESSION}}-unaggregated-expression-data-${{metric}}.jsonl.bz2
                echo "Checking $JSONL to upload..."
                if [ -f $JSONL ] && [ ! -f $UPLOADED_FILE ]; then
                    echo "...uploading"
                    gsutil cp $JSONL $GS_PREFIX/unaggregated_data/$ACCESSION/${{ACCESSION}}-unaggregated-expression-data-${{metric}}.jsonl.bz2
                    touch $UPLOADED_FILE
                fi
            done
        done
        """

rule aggregate_ot_baseline_exports:
    input: aggregate_accessions_ot_baseline_exports
    output: "ot_baseline_exports.done"
    shell:
        """
        touch {output}
        """

rule aggregate_ot_baseline_uploads:
    input: aggregate_accessions_ot_baseline_uploads
    output: "ot_baseline_uploads.done"
    shell:
        """
        touch {output}
        """
