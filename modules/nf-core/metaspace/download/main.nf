process METASPACE_DOWNLOAD {
    label 'process_low'
    container 'docker.io/bwadie/metaspace_converter'

    input:
    tuple val(dataset_id), val(database), val(version)

    output:
    path "${dataset_id}_*.csv", optional: true, emit: results
    stdout emit: log  // check meta.yml for see how to use!
    path 'versions.yml', emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    template 'metaspace_download.py'
}
