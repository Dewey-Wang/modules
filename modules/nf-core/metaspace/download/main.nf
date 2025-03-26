#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.container = 'docker.io/bwadie/metaspace_converter'
params.output = 'results'
params.input_csv = false
params.datasets = false

process METASPACE_DOWNLOAD {
    label 'process_low'
    publishDir "${params.output}", mode: 'copy', overwrite: true
    container "${params.container}"

    input:
    tuple val(dataset_id), val(database), val(version)

    output:
    path "${dataset_id}_*.csv", optional: true, emit: results
    stdout emit: log

    script:
    template 'metaspace_download.py'
}

workflow {
    if (!params.input_csv && !params.datasets) {
        error "Must provide either 'input_csv' or 'datasets' parameter."
    }

    if (params.input_csv) {
        input_csv = file("${params.input_csv}", checkIfExists: true)
        datasets = Channel.fromPath(input_csv)
            .splitCsv(header: true, strip: true)
            .map { row ->
                def dataset_id = row.dataset_id?.trim() ?: null
                def database = row.database?.trim() ?: null
                def version = row.version?.trim() ?: null
                [dataset_id, database, version]
            }
    } else {
        datasets = Channel.fromList("${params.datasets}")
    }

    METASPACE_DOWNLOAD(datasets)
    METASPACE_DOWNLOAD.out.log.view { "${it.split('\n').last().trim()}" }
    METASPACE_DOWNLOAD.out.results.last().subscribe {
        println "All the output saved in: ${workflow.launchDir}/${params.output}"
    }
}
