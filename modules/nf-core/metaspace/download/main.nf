process METASPACE_DOWNLOAD {
    label 'process_low'

    publishDir 'results', mode: 'copy'
    container 'bwadie/metaspace_converter'

    input:
    tuple val(dataset_id), val(database), val(version)
    path script_file

    output:
    path "${dataset_id}_*.csv", optional: true, emit: results  // 設為可選
    stdout emit: log  // 將 dataset_id 與 stdout 一起輸出

    script:
    """
    python3 $script_file \\
        --dataset_id "$dataset_id" \\
        --database "${database ?: 'None'}" \\
        --version "${version ?: 'None'}"
    """
}

workflow {
    script_file = file("${projectDir}/metaspace_download.py")

    if (params.datasets_file) {
        datasets = Channel.fromPath(params.datasets_file)
            .splitCsv(header: true, strip: true)
            .map { row ->
                // 將空值轉換為 null
                def database = row.database?.trim() ?: null
                def version = row.version?.trim() ?: null
                [row.dataset_id, database, version]
            }
    } else {
        datasets = Channel.fromList(params.datasets)
    }

    METASPACE_DOWNLOAD(datasets, script_file)
    METASPACE_DOWNLOAD.out.log.view { "${it.split('\n').last().trim()}" }
}
